import sources_neon as N
import agg

def test_delivery_dict_with_error():
    d = N._delivery_dict("undeliverable", "EC_FREQUENCY_CAPPING", 7032)
    assert d["status"] == "undeliverable"
    assert d["error_name"] == "EC_FREQUENCY_CAPPING (code 7032)"
    assert agg.err_bucket(d["error_name"]) == "freq_cap"

def test_delivery_dict_no_error():
    d = N._delivery_dict("delivered", None, 0)
    assert d["status"] == "delivered" and d["error_name"] == "No Error (code 0)"
    assert agg.err_bucket(d["error_name"]) == "entregado"

def test_delivery_dict_invalido_code_351():
    d = N._delivery_dict("undeliverable", "EC_INVALID_DESTINATION", 351)
    assert agg.err_bucket(d["error_name"]) == "invalido"


# --- cache incremental de historia congelada (2026-08-18) ---

def _fake_db(monkeypatch, tmp_path, rows):
    """Simula Neon: _rows aplica el filtro de fecha >= del SQL real; _scalar cuenta la región < cutoff."""
    monkeypatch.setattr(N, "_CACHE_DIR", str(tmp_path))
    calls = {"rows": 0}
    def fake_rows(sql, args=()):
        calls["rows"] += 1
        since = args[-1]  # el último parámetro del camino cacheado es el cutoff ::date
        return [dict(r) for r in rows if r["attempted_at"][:10] >= since]
    def fake_scalar(sql, args=()):
        cut = args[-1]
        return sum(1 for r in rows if r["attempted_at"][:10] < cut)
    monkeypatch.setattr(N, "_rows", fake_rows)
    monkeypatch.setattr(N, "_scalar", fake_scalar)
    return calls

def test_send_log_cache_incremental(monkeypatch, tmp_path):
    import datetime
    hoy = datetime.date.today()
    d = lambda n: (hoy - datetime.timedelta(days=n)).isoformat() + " 09:30:00"
    rows = [{"nid": i, "phone": f"55{i}", "template": "t", "message_id": f"m{i}",
             "accepted": True, "attempted_at": d(n)} for i, n in enumerate([40, 10, 2, 0])]
    calls = _fake_db(monkeypatch, tmp_path, rows)
    r1 = N.send_log_rows(country="MX")            # cold: fetch completo, congela < hoy-3
    r2 = N.send_log_rows(country="MX")            # warm: solo >= hoy-3 viaja
    key = lambda rs: sorted((x["message_id"], x["attempted_at"]) for x in rs)
    assert key(r1) == key(r2) == key(rows)
    assert calls["rows"] == 2                     # una consulta por build (más el COUNT barato)

def test_send_log_cache_guard_refetch_si_historia_cambia(monkeypatch, tmp_path):
    import datetime
    hoy = datetime.date.today()
    d = lambda n: (hoy - datetime.timedelta(days=n)).isoformat() + " 09:30:00"
    rows = [{"nid": 1, "phone": "551", "template": "t", "message_id": "m1",
             "accepted": True, "attempted_at": d(30)}]
    _fake_db(monkeypatch, tmp_path, rows)
    N.send_log_rows(country="MX")                                  # puebla cache
    rows.append({"nid": 2, "phone": "552", "template": "t", "message_id": "m2",
                 "accepted": True, "attempted_at": d(20)})         # cambió la región congelada
    r = N.send_log_rows(country="MX")                              # guard detecta y refetchea
    assert {x["message_id"] for x in r} == {"m1", "m2"}
