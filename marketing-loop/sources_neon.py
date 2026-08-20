import os, psycopg

# tz local por país para que el bucketeo por fecha sea hora local, no UTC.
TZ = {"MX": "America/Mexico_City", "CO": "America/Bogota"}

def _db_url():
    """DATABASE_URL manda sobre NEON_DATABASE_URL — misma precedencia que config.db_url()
    del motor (marketing-loop-sellers), para que el cutover a Supabase sea agregar la var
    y el rollback borrarla, sin tocar código ni acá ni allá."""
    return os.environ.get("DATABASE_URL") or os.environ["NEON_DATABASE_URL"]

def _rows(sql, args=()):
    with psycopg.connect(_db_url()) as c:
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
    with psycopg.connect(_db_url()) as c:
        cur=c.execute(q, tuple(args)); cols=[d.name for d in cur.description]
        rows=[[_mask_pii(col, v) for col,v in zip(cols, r)] for r in cur.fetchall()]
    return {"cols": cols, "rows": rows}

def send_log_rows(days=None, country=None):
    # attempted_at convertido a tz local del país (country=None -> comportamiento actual: sin filtro, tz Mexico_City).
    tz = TZ.get(country, "America/Mexico_City")
    q=f"SELECT nid,deal_id,phone,line,template,message_id,api_http_code,accepted,(attempted_at AT TIME ZONE '{tz}')::text AS attempted_at FROM send_log"
    where=[]; args=[]
    if country: where.append("country=%s"); args.append(country)
    if days: where.append(f"attempted_at >= now() - make_interval(days => {int(days)})")
    if where: q += " WHERE " + " AND ".join(where)
    return _rows(q, tuple(args))

def recreation_rows(country=None):
    tz = TZ.get(country, "America/Mexico_City")
    q=f"SELECT old_nid,orig_deal_id,new_deal_id,new_nid,state_at_creation,http_code,success,responded_at::text,(created_at AT TIME ZONE '{tz}')::text AS created_at FROM recreation"
    args=[]
    if country: q += " WHERE country=%s"; args=[country]
    return _rows(q, tuple(args))

def contact_status_rows(country=None):
    q="SELECT phone,state,attempt_count,first_sent_at::text,last_sent_at::text,last_delivered_at::text,responded_at::text,reason FROM contact_status"
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
    """Entrega persistida por el motor en send_log (durable, sin lag). {message_id: {status,error_name}}."""
    q = ("SELECT message_id, delivery_status, error_name, error_id FROM send_log "
         "WHERE message_id IS NOT NULL AND delivery_status IS NOT NULL")
    args = []
    if country:
        q += " AND country=%s"; args.append(country)
    return {r["message_id"]: _delivery_dict(r["delivery_status"], r["error_name"], r["error_id"])
            for r in _rows(q, tuple(args))}
