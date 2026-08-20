#!/usr/bin/env python3
"""Construye el data.json del tablero de Pauta Propiedades.com en la nube.

Corre en el GitHub Action del hub (update-pauta.yml), sin acceso a la máquina
local. Es autocontenido: NO depende de los registro.json locales, sino del
`catalogo.json` versionado (mapeo ad_id -> inmueble/cliente) que se exporta a
mano desde local cuando publicamos.

Fuentes:
  - catalogo.json (local del repo)          -> etiqueta cada ad con su inmueble
  - Marketing API de Meta (token = secret)  -> métricas + estado live por ad
  - Google Sheet 'Pauta PCOM' (público)     -> nombres/presupuesto por cliente

Variables de entorno:
  META_PCOM_TOKEN        token de sistema con acceso a la cuenta de pauta
  META_PCOM_AD_ACCOUNT   act_XXXXXXXX de la cuenta de Propiedades.com
  DATABASE_URL           conexión al ledger de eventos (bitácora); opcional. Si falta,
                         se usa NEON_DATABASE_URL (misma precedencia que el motor)
"""
import datetime
import json
import os

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CATALOGO = os.path.join(HERE, "catalogo.json")
CLIENTES = os.path.join(HERE, "clientes.json")
DATA_OUT = os.path.join(HERE, "data.json")

API_VERSION = "v21.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"
TOKEN = os.environ.get("META_PCOM_TOKEN", "")
AD_ACCOUNT = os.environ.get("META_PCOM_AD_ACCOUNT", "")

# Card pública de Metabase "Inventario FUNDADORES": estatus del listing por
# propiedad (Activo / Eliminado / Vencido / …) para los 3 clientes fundadores.
METABASE_CARD = "51604d7d-00ed-46af-8223-78a0a01b940d"
METABASE_URL = f"https://metabase.propiedades.com/api/public/card/{METABASE_CARD}/query/json"

INSIGHT_FIELDS = ("ad_id,spend,impressions,reach,clicks,inline_link_clicks,"
                  "ctr,cpc,cpm,actions,cost_per_action_type")


# ---------- Meta ----------
def _check(resp):
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"respuesta no-JSON ({resp.status_code}): {resp.text[:300]}")
    if resp.status_code >= 400 or "error" in data:
        err = data.get("error", data)
        raise RuntimeError(f"{err.get('code')} {err.get('message')}".strip())
    return data


def fetch_insights(date_preset="maximum"):
    """Insights a nivel ad de toda la cuenta -> dict ad_id -> métricas."""
    url = f"{BASE}/{AD_ACCOUNT}/insights"
    params = {"level": "ad", "fields": INSIGHT_FIELDS,
              "date_preset": date_preset, "limit": 500, "access_token": TOKEN}
    rows = []
    while url:
        data = _check(requests.get(url, params=params, timeout=90))
        rows.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        params = None  # el 'next' ya trae el querystring completo
    return {r["ad_id"]: r for r in rows}


def fetch_status():
    """Estado efectivo (delivery) live por ad -> dict ad_id -> effective_status."""
    url = f"{BASE}/{AD_ACCOUNT}/ads"
    params = {"fields": "id,effective_status", "limit": 500, "access_token": TOKEN}
    out = {}
    while url:
        data = _check(requests.get(url, params=params, timeout=90))
        for r in data.get("data", []):
            out[r["id"]] = r.get("effective_status", "")
        url = data.get("paging", {}).get("next")
        params = None
    return out


def fetch_daily(ad_ids, date_preset="maximum"):
    """Serie diaria por ad (time_increment=1) -> lista de filas.
    Alimenta la tabla por período (día/semana/ciclo/mes) del tablero, que
    bucketea del lado del cliente. Solo se conservan los ads del catálogo."""
    url = f"{BASE}/{AD_ACCOUNT}/insights"
    params = {"level": "ad", "fields": "ad_id,spend,impressions,inline_link_clicks",
              "time_increment": 1, "date_preset": date_preset, "limit": 500,
              "access_token": TOKEN}
    serie = []
    while url:
        data = _check(requests.get(url, params=params, timeout=90))
        for r in data.get("data", []):
            if r["ad_id"] not in ad_ids:
                continue
            serie.append({
                "ad_id": r["ad_id"],
                "fecha": r["date_start"],
                "gasto": round(_f(r.get("spend")), 2),
                "impresiones": _i(r.get("impressions")),
                "clics_enlace": _i(r.get("inline_link_clicks")),
            })
        url = data.get("paging", {}).get("next")
        params = None
    return serie


def event_metrics(actions, cost_per_action, spend, custom_event="interes_marketing"):
    """Extrae (nº eventos, CPA) de los custom conversions offsite del pixel.
    Suma cualquier action_type 'offsite_conversion.custom.*' (la cuenta tiene un
    único custom event). CPA = promedio ponderado; fallback spend/eventos."""
    ev = 0
    for a in actions or []:
        if str(a.get("action_type", "")).startswith("offsite_conversion.custom."):
            ev += int(float(a.get("value", 0) or 0))
    cpa = 0.0
    for a in cost_per_action or []:
        if str(a.get("action_type", "")).startswith("offsite_conversion.custom."):
            cpa = round(float(a.get("value", 0) or 0), 2)
            break
    if not cpa and ev:
        cpa = round(float(spend) / ev, 2)
    return ev, cpa


# ---------- Metabase: estatus del listing + leads diarios ----------
def fetch_metabase():
    """Filas crudas de la card pública (property_id, Estatus, Fecha, Leads).
    Falla suave: si Metabase no responde, devuelve []."""
    try:
        return requests.get(METABASE_URL, timeout=60).json()
    except Exception as e:   # noqa: BLE001 — no romper el build por Metabase
        print(f"  ⚠ Metabase no disponible ({e}); estatus/leads = sin dato")
        return []


def leads_prepost(mb_rows, ad_start, today):
    """Por property_id: (leads_antes, dias_antes, leads_pauta, dias_pauta).
    'antes' = [primer día con leads … inicio de pauta); 'pauta' = [inicio … ayer].
    Excluye hoy (parcial). ad_start: dict property_id -> fecha ISO de 1er gasto."""
    leads = {}          # pid -> {fecha: leads}
    fechas = []
    for r in mb_rows:
        f = r.get("Fecha")
        if not f:
            continue
        pid = str(r["Property_id"])
        leads.setdefault(pid, {})[f] = leads.get(pid, {}).get(f, 0) + (r.get("Leads") or 0)
        fechas.append(f)
    if not fechas:
        return {}
    lm = datetime.date.fromisoformat(min(fechas))
    yest = today - datetime.timedelta(days=1)
    out = {}
    for pid, st in ad_start.items():
        st_d = datetime.date.fromisoformat(st)
        la = lp = 0
        for f, n in leads.get(pid, {}).items():
            fd = datetime.date.fromisoformat(f)
            if fd < st_d:
                la += n
            elif fd <= yest:
                lp += n
        out[pid] = (la, max((st_d - lm).days, 0), lp, max((yest - st_d).days + 1, 0))
    return out


# ---------- Ledger de eventos (Neon) ----------
def fetch_bitacora(limite=200):
    """Últimos eventos del ledger + resumen por tipo. Falla suave."""
    import ledger
    if not ledger.db_url():
        print("  ⚠ DATABASE_URL / NEON_DATABASE_URL ausente; bitácora vacía")
        return {"eventos": [], "resumen": {}}
    try:
        conn = ledger.connect()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT ts, tipo, ad_id, id_aviso, cliente_id, razon, fuente,
                       detalle->>'reconstruido'
                FROM {ledger.SCHEMA}.eventos
                ORDER BY ts DESC LIMIT %s""", (limite,))
            evs = [{"ts": r[0].isoformat(), "tipo": r[1], "ad_id": r[2],
                    "id_aviso": r[3], "cliente_id": r[4], "razon": r[5],
                    "fuente": r[6],
                    "reconstruido": (r[7] == "true")} for r in cur.fetchall()]
            cur.execute(f"SELECT tipo, count(*) FROM {ledger.SCHEMA}.eventos "
                        f"GROUP BY tipo")
            resumen = {t: n for t, n in cur.fetchall()}
        conn.close()
        return {"eventos": evs, "resumen": resumen}
    except Exception as e:  # noqa: BLE001 — no romper el build por el ledger
        print(f"  ⚠ ledger no disponible ({e}); bitácora vacía")
        return {"eventos": [], "resumen": {}}


# ---------- Google Sheet ----------
def load_config():
    """Config de campaña desde clientes.json (reemplaza el Google Sheet).
    Devuelve (clientes, cfg) donde clientes: cid -> {nombre, presupuesto_diario, activo}."""
    cfg = json.load(open(CLIENTES, encoding="utf-8"))
    clientes = {c["cliente_id"]: {"nombre": c["nombre"],
                                  "presupuesto_diario": float(c.get("presupuesto_diario", 0)),
                                  "activo": bool(c.get("activo", True))}
                for c in cfg.get("clientes", [])}
    return clientes, cfg


def _f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _i(v, d=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return d


def build(date_preset="maximum"):
    cat = json.load(open(CATALOGO, encoding="utf-8"))
    pubs = cat["publicaciones"]
    clientes, cfg = load_config()
    ins = fetch_insights(date_preset)
    status_live = fetch_status()
    serie = fetch_daily({p["ad_id"] for p in pubs}, date_preset)

    mb_rows = fetch_metabase()
    listing_status = {str(r["Property_id"]): (r.get("Estatus") or "Sin dato")
                      for r in mb_rows if r.get("Property_id") is not None}
    # inicio de pauta por propiedad = primer día con gasto > 0
    adprop = {p["ad_id"]: str(p["id_aviso"]) for p in pubs}
    ad_start = {}
    for s in serie:
        if s["gasto"] > 0:
            pid = adprop.get(s["ad_id"])
            if pid and (pid not in ad_start or s["fecha"] < ad_start[pid]):
                ad_start[pid] = s["fecha"]
    prepost = leads_prepost(mb_rows, ad_start, datetime.date.today())

    bitacora = fetch_bitacora()
    for ev in bitacora["eventos"]:
        ev["cliente"] = clientes.get(ev["cliente_id"], {}).get("nombre", ev["cliente_id"])

    filas = []
    for p in pubs:
        m = ins.get(p["ad_id"], {})
        spend = _f(m.get("spend"))
        link_clicks = _i(m.get("inline_link_clicks"))
        la, da, lp, dp = prepost.get(str(p["id_aviso"]), (0, 0, 0, 0))
        ev, cpa = event_metrics(m.get("actions"), m.get("cost_per_action_type"), spend)
        filas.append({
            **p,
            "status": status_live.get(p["ad_id"], p.get("status", "")),
            "estatus_listing": listing_status.get(str(p["id_aviso"]), "Sin dato"),
            "leads_antes": la, "dias_antes": da,
            "leads_pauta": lp, "dias_pauta": dp,
            "cliente": clientes.get(p["cliente_id"], {}).get("nombre", p["cliente_id"]),
            "gasto": round(spend, 2),
            "impresiones": _i(m.get("impressions")),
            "alcance": _i(m.get("reach")),
            "clics": _i(m.get("clicks")),
            "clics_enlace": link_clicks,
            "ctr": round(_f(m.get("ctr")), 2),
            "cpc": round(_f(m.get("cpc")), 2),
            "costo_x_clic_enlace": round(spend / link_clicks, 2) if link_clicks else 0.0,
            "etapa": p.get("etapa", "traffic"),
            "eventos": ev,
            "cpa": cpa,
        })

    filas.sort(key=lambda x: x["gasto"], reverse=True)

    def agg(rows):
        return (sum(r["gasto"] for r in rows),
                sum(r["impresiones"] for r in rows),
                sum(r["clics_enlace"] for r in rows))

    g, imp, cl = agg(filas)
    objetivo = int(cfg.get("ads_activos_objetivo", 20))
    min_usd = float(cfg.get("min_usd_por_ad", 1)) or 1
    cli_rows = []
    for cid, c in clientes.items():
        rs = [r for r in filas if r["cliente_id"] == cid]
        if not rs:
            continue
        cg, cimp, ccl = agg(rs)
        # meta de ads activos: min(objetivo, presupuesto/$min) — respeta el $1/día
        meta_activos = min(objetivo, int(c["presupuesto_diario"] // min_usd))
        cli_rows.append({
            "cliente_id": cid, "nombre": c["nombre"],
            "presupuesto_diario": c["presupuesto_diario"],
            "activo": c.get("activo", True),
            "meta_activos": meta_activos,
            "gasto": round(cg, 2), "impresiones": cimp, "clics_enlace": ccl,
            "ctr": round(ccl / cimp * 100, 2) if cimp else 0.0,
            "cpc": round(cg / ccl, 2) if ccl else 0.0,
            "publicaciones": len(rs),
            "activas": sum(1 for r in rs if r["status"] == "ACTIVE"),
        })
    cli_rows.sort(key=lambda x: x["gasto"], reverse=True)

    data = {
        "generated_at_iso": os.environ.get("BUILD_TS")
        or datetime.datetime.now().isoformat(),
        "date_preset": date_preset,
        "summary": {
            "gasto": round(g, 2), "impresiones": imp, "clics_enlace": cl,
            "ctr": round(cl / imp * 100, 2) if imp else 0.0,
            "cpc": round(g / cl, 2) if cl else 0.0,
            "publicaciones": len(filas),
            "activas": sum(1 for r in filas if r["status"] == "ACTIVE"),
            "con_gasto": sum(1 for r in filas if r["gasto"] > 0),
        },
        "config_campana": {
            "actualizado": cfg.get("actualizado"),
            "ads_activos_objetivo": objetivo,
            "min_usd_por_ad": min_usd,
        },
        "clientes": cli_rows,
        "publicaciones": filas,
        "serie": serie,
        "bitacora": bitacora,
    }
    with open(DATA_OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    s = data["summary"]
    print(f"→ {DATA_OUT}")
    print(f"  {s['publicaciones']} pubs · {s['activas']} activas · "
          f"${s['gasto']} · {s['impresiones']} impr · {s['clics_enlace']} clics")


if __name__ == "__main__":
    build(os.environ.get("PAUTA_DATE_PRESET", "maximum"))
