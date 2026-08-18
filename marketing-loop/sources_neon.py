import datetime, json, os, psycopg

# tz local por país para que el bucketeo por fecha sea hora local, no UTC.
TZ = {"MX": "America/Mexico_City", "CO": "America/Bogota"}

# --- Cache incremental de historia CONGELADA (ahorro de network transfer de Neon, 2026-08-18) ---
# send_log es append-only y sus columnas de envío son inmutables tras el insert; delivery_status/
# error solo mutan hasta ~21 días (persist /logs ~3d + backfill del mart 21d). Todo lo anterior a
# FREEZE_DAYS es historia muerta: se guarda UNA vez en disco y cada build solo trae lo reciente.
# Guardas: un COUNT(*) barato sobre la región congelada debe cuadrar con el archivo; si no
# (borrado/backfill manual), se refetchea completo. Escape: borrar marketing-loop/.neon_cache/.
# send_log (columnas de envío): inmutables desde el insert -> congelar casi todo (3 días de
# margen por tz/reloj). delivery_status/error: mutan hasta ~21 días (backfill) -> 35.
FREEZE_DAYS_SEND = 3
FREEZE_DAYS_DELIVERY = 35
_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".neon_cache")

def _cache_load(name):
    try:
        with open(os.path.join(_CACHE_DIR, name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None

def _cache_save(name, obj):
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        tmp = os.path.join(_CACHE_DIR, name + ".tmp")
        with open(tmp, "w") as f:
            json.dump(obj, f)
        os.replace(tmp, os.path.join(_CACHE_DIR, name))
    except OSError:
        pass  # best-effort: sin cache solo se pierde el ahorro

def _freeze_cutoff(days):
    return (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

def _scalar(sql, args=()):
    with psycopg.connect(os.environ["NEON_DATABASE_URL"]) as c:
        return c.execute(sql, args).fetchone()[0]

def _rows(sql, args=()):
    with psycopg.connect(os.environ["NEON_DATABASE_URL"]) as c:
        cur=c.execute(sql,args); cols=[d.name for d in cur.description]
        return [dict(zip(cols,r)) for r in cur.fetchall()]

def _mask_pii(col, v):
    """El data.json se sirve PÚBLICO (GitHub Pages) -> enmascarar PII en la muestra (solo explicación).
    Teléfono: deja los últimos 4 dígitos. Email: deja 2 chars + dominio."""
    if v is None: return None
    s=str(v); lc=col.lower()
    if lc in ("phone","telefono","telefono_2","telefono_3","celular"):
        dig="".join(ch for ch in s if ch.isdigit())
        return ("•"*max(0,len(dig)-4)+dig[-4:]) if dig else s
    if lc in ("email","correo"):
        at=s.find("@")
        return (s[:2]+"•••"+s[at:]) if at>0 else (s[:2]+"•••")
    return s

def tabla_muestra(tabla, order_col, country=None, n=5):
    """Últimos `n` registros de una tabla de Neon con TODAS las columnas (a modo de explicación/muestra).
    tabla y order_col son valores de confianza (definidos en build_data, no input externo). PII enmascarada."""
    q=f"SELECT * FROM {tabla}"
    args=[]
    if country: q+=" WHERE country=%s"; args=[country]
    q+=f" ORDER BY {order_col} DESC NULLS LAST LIMIT {int(n)}"
    with psycopg.connect(os.environ["NEON_DATABASE_URL"]) as c:
        cur=c.execute(q, tuple(args)); cols=[d.name for d in cur.description]
        rows=[[_mask_pii(col, v) for col,v in zip(cols, r)] for r in cur.fetchall()]
    return {"cols": cols, "rows": rows}

# deal_id/line/api_http_code dejaron de viajar (2026-08-18): ningún consumidor de build_data/agg
# los lee y eran ~1/3 del peso de la tabla completa por país.
_SL_COLS = "nid,phone,template,message_id,accepted"

def send_log_rows(days=None, country=None):
    # attempted_at convertido a tz local del país (country=None -> comportamiento actual: sin filtro, tz Mexico_City).
    tz = TZ.get(country, "America/Mexico_City")
    sel = f"SELECT {_SL_COLS},(attempted_at AT TIME ZONE '{tz}')::text AS attempted_at FROM send_log"
    if days or not country:
        # camino directo (compat; el build diario usa el cacheado de abajo)
        where=[]; args=[]
        if country: where.append("country=%s"); args.append(country)
        if days: where.append(f"attempted_at >= now() - make_interval(days => {int(days)})")
        if where: sel += " WHERE " + " AND ".join(where)
        return _rows(sel, tuple(args))
    # camino CACHEADO (build diario, days=None): las filas de send_log son inmutables tras el
    # insert (la entrega vive en delivery_by_msgid) -> la historia > FREEZE_DAYS_SEND viene del
    # disco y de Neon solo viaja lo reciente. Guard: COUNT de la región congelada debe cuadrar.
    name = f"send_log-{country}.json"
    cutoff = _freeze_cutoff(FREEZE_DAYS_SEND)
    cached = _cache_load(name)
    frozen, old_cutoff = [], "1900-01-01"
    if cached and cached.get("cutoff", "") <= cutoff:
        n = _scalar("SELECT COUNT(*) FROM send_log WHERE country=%s "
                    "AND (attempted_at AT TIME ZONE %s)::date < %s::date",
                    (country, tz, cached["cutoff"]))
        if n == len(cached["rows"]):
            frozen, old_cutoff = cached["rows"], cached["cutoff"]
    fresh = _rows(sel + " WHERE country=%s AND (attempted_at AT TIME ZONE %s)::date >= %s::date",
                  (country, tz, old_cutoff))
    _cache_save(name, {"cutoff": cutoff,
                       "rows": frozen + [r for r in fresh if (r.get("attempted_at") or "")[:10] < cutoff]})
    return frozen + fresh

def recreation_rows(country=None):
    tz = TZ.get(country, "America/Mexico_City")
    q=f"SELECT old_nid,orig_deal_id,new_deal_id,new_nid,state_at_creation,http_code,success,responded_at::text,(created_at AT TIME ZONE '{tz}')::text AS created_at FROM recreation"
    args=[]
    if country: q += " WHERE country=%s"; args=[country]
    return _rows(q, tuple(args))

def contact_status_rows(country=None):
    # Solo phone+state viajan (2026-08-18): los consumidores (terminal_phones y agg.contact_dist)
    # no leen nada más; las 6 columnas extra eran ~70% del peso (3.7 MB/día CO + 2.9 MX).
    q="SELECT phone,state FROM contact_status"
    args=[]
    if country: q += " WHERE country=%s"; args=[country]
    return _rows(q, tuple(args))

def _delivery_dict(delivery_status, error_name, error_id):
    """Forma la entrada de mbm desde una fila de send_log. error_name con el formato
    '<NAME> (code <ID>)' que agg.err_bucket parsea (igual que mart/Infobip)."""
    if error_id in (None, 0):
        ename = "No Error (code 0)"
    else:
        ename = f"{error_name} (code {error_id})"
    return {"status": delivery_status, "error_name": ename}

def delivery_by_msgid(country=None):
    """Entrega persistida por el motor en send_log (durable, sin lag). {message_id: {status,error_name}}.
    Con país usa el cache congelado: delivery_status/error solo mutan hasta ~21 días
    (persist /logs + backfill del mart); lo anterior a FREEZE_DAYS_DELIVERY viaja UNA sola vez."""
    if not country:
        return {r["message_id"]: _delivery_dict(r["delivery_status"], r["error_name"], r["error_id"])
                for r in _rows("SELECT message_id, delivery_status, error_name, error_id FROM send_log "
                               "WHERE message_id IS NOT NULL AND delivery_status IS NOT NULL")}
    tz = TZ.get(country, "America/Mexico_City")
    name = f"delivery-{country}.json"
    cutoff = _freeze_cutoff(FREEZE_DAYS_DELIVERY)
    cached = _cache_load(name)
    frozen, old_cutoff = {}, "1900-01-01"
    if cached and cached.get("cutoff", "") <= cutoff:
        n = _scalar("SELECT COUNT(*) FROM send_log WHERE message_id IS NOT NULL "
                    "AND delivery_status IS NOT NULL AND country=%s "
                    "AND (attempted_at AT TIME ZONE %s)::date < %s::date",
                    (country, tz, cached["cutoff"]))
        if n == len(cached["items"]):
            frozen, old_cutoff = cached["items"], cached["cutoff"]
    fresh = _rows("SELECT message_id, delivery_status, error_name, error_id, "
                  "(attempted_at AT TIME ZONE %s)::date::text AS d FROM send_log "
                  "WHERE message_id IS NOT NULL AND delivery_status IS NOT NULL AND country=%s "
                  "AND (attempted_at AT TIME ZONE %s)::date >= %s::date",
                  (tz, country, tz, old_cutoff))
    items, new_frozen = dict(frozen), dict(frozen)
    for r in fresh:
        t = [r["delivery_status"], r["error_name"], r["error_id"]]
        items[r["message_id"]] = t
        if r["d"] < cutoff:
            new_frozen[r["message_id"]] = t
    _cache_save(name, {"cutoff": cutoff, "items": new_frozen})
    return {mid: _delivery_dict(*t) for mid, t in items.items()}
