"""Ledger de eventos de pauta -> Neon (schema `pauta`).

Append-only, a nivel ad. Tolerante a fallos: si Neon no responde al escribir,
avisa por stderr y NO rompe la operación de pauta.
"""
import datetime
import json
import os
import sys

import psycopg

SCHEMA = os.environ.get("PAUTA_SCHEMA", "pauta")
TIPOS = {"CREATED", "ACTIVATED", "PAUSED", "DELETED", "BUDGET_CHANGED"}


def db_url():
    """DATABASE_URL manda sobre NEON_DATABASE_URL — misma precedencia que config.db_url()
    del motor, para que el cutover a Supabase sea agregar la var y el rollback borrarla."""
    return os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL", "")


def connect():
    url = db_url()
    if not url:
        raise RuntimeError("DATABASE_URL / NEON_DATABASE_URL no definido")
    return psycopg.connect(url)


def build_event(tipo, cliente_id, ad_id=None, id_aviso=None, razon=None,
                detalle=None, fuente="manual", ts=None):
    if tipo not in TIPOS:
        raise ValueError(f"tipo inválido: {tipo} (válidos: {sorted(TIPOS)})")
    if cliente_id in (None, ""):
        raise ValueError("cliente_id requerido")
    return {
        "ts": ts or datetime.datetime.now(datetime.timezone.utc),
        "tipo": tipo,
        "ad_id": str(ad_id) if ad_id is not None else None,
        "id_aviso": str(id_aviso) if id_aviso is not None else None,
        "cliente_id": str(cliente_id),
        "razon": razon,
        "detalle": detalle or {},
        "fuente": fuente,
    }


def ensure_schema(conn=None):
    close = conn is None
    conn = conn or connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.eventos (
                    id         BIGSERIAL PRIMARY KEY,
                    ts         TIMESTAMPTZ NOT NULL,
                    tipo       TEXT NOT NULL,
                    ad_id      TEXT,
                    id_aviso   TEXT,
                    cliente_id TEXT NOT NULL,
                    razon      TEXT,
                    detalle    JSONB NOT NULL DEFAULT '{{}}',
                    fuente     TEXT NOT NULL,
                    creado_en  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (ad_id, ts, tipo)
                );""")
            cur.execute(f"CREATE INDEX IF NOT EXISTS eventos_ad_ts "
                        f"ON {SCHEMA}.eventos (ad_id, ts);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS eventos_cli_ts "
                        f"ON {SCHEMA}.eventos (cliente_id, ts);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS eventos_tipo_ts "
                        f"ON {SCHEMA}.eventos (tipo, ts);")
            cur.execute(f"""
                CREATE OR REPLACE VIEW {SCHEMA}.ad_estado_actual AS
                SELECT DISTINCT ON (ad_id)
                       ad_id, id_aviso, cliente_id,
                       tipo AS ultimo_evento, ts AS ultima_ts, razon, fuente
                FROM {SCHEMA}.eventos
                WHERE ad_id IS NOT NULL
                ORDER BY ad_id, ts DESC;""")
        conn.commit()
    finally:
        if close:
            conn.close()


def insert_events(events, conn=None):
    """Inserta eventos (dicts de build_event). Idempotente por
    UNIQUE(ad_id, ts, tipo) via ON CONFLICT DO NOTHING. Devuelve nº insertadas."""
    if not events:
        return 0
    close = conn is None
    conn = conn or connect()
    n = 0
    try:
        with conn.cursor() as cur:
            for e in events:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.eventos
                      (ts, tipo, ad_id, id_aviso, cliente_id, razon, detalle, fuente)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ad_id, ts, tipo) DO NOTHING""",
                    (e["ts"], e["tipo"], e["ad_id"], e["id_aviso"],
                     e["cliente_id"], e["razon"], json.dumps(e["detalle"]),
                     e["fuente"]))
                n += cur.rowcount
        conn.commit()
    finally:
        if close:
            conn.close()
    return n


def log_event(tipo, cliente_id, **kw):
    """Construye e inserta un evento. Tolerante a fallos de Neon (avisa y
    devuelve False). Los errores de validación (ValueError) SÍ relanzan."""
    ev = build_event(tipo, cliente_id, **kw)   # ValueError -> relanza
    try:
        insert_events([ev])
        return True
    except Exception as e:  # noqa: BLE001 — no romper la pauta por el ledger
        print(f"  ⚠ ledger: no se pudo registrar {tipo} "
              f"(ad {ev['ad_id']}, cli {cliente_id}): {e}", file=sys.stderr)
        return False
