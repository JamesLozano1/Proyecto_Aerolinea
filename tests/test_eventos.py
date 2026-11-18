from datetime import date

import pandas as pd

from ingesta.eventos import (ARRIVAL, CANCELLATION, DEPARTURE, DIVERSION, fila_a_eventos,
                             hora_local_a_utc, parsear_fecha)

ZONAS = {"ATL": "America/New_York", "LAX": "America/Los_Angeles"}


def fila(**kw):
    base = {"FL_DATE": "2026-01-15", "OP_UNIQUE_CARRIER": "DL", "TAIL_NUM": "N123DL",
            "OP_CARRIER_FL_NUM": 1234, "ORIGIN": "ATL", "DEST": "LAX", "CRS_DEP_TIME": 1900,
            "DEP_DELAY": 45.0, "ARR_DELAY": 20.0, "CANCELLED": 0.0, "CANCELLATION_CODE": None,
            "DIVERTED": 0.0, "CRS_ELAPSED_TIME": 275.0, "ACTUAL_ELAPSED_TIME": 250.0,
            "DISTANCE": 1946.0, "CARRIER_DELAY": 0.0, "WEATHER_DELAY": 0.0, "NAS_DELAY": 0.0,
            "SECURITY_DELAY": 0.0, "LATE_AIRCRAFT_DELAY": 20.0}
    base.update(kw)
    return base


def test_vuelo_normal_en_utc():
    dep, arr = fila_a_eventos(fila(), ZONAS)
    # 19:00 EST (UTC-5) = 00:00Z del día siguiente; + 45 min de retraso
    assert dep["event_type"] == DEPARTURE and dep["airport"] == "ATL"
    assert dep["event_ts"] == "2026-01-16T00:45:00Z" and dep["delay_minutes"] == 45
    assert dep["sched_dep_utc"] == "2026-01-16T00:00:00Z"
    # llegada = salida real + 250 min
    assert arr["event_type"] == ARRIVAL and arr["airport"] == "LAX"
    assert arr["event_ts"] == "2026-01-16T04:55:00Z" and arr["delay_minutes"] == 20
    assert arr["late_aircraft_delay"] == 20


def test_cancelado():
    evs = fila_a_eventos(fila(CANCELLED=1.0, CANCELLATION_CODE="B", TAIL_NUM=None,
                              DEP_DELAY=None, ARR_DELAY=None, ACTUAL_ELAPSED_TIME=None), ZONAS)
    assert len(evs) == 1 and evs[0]["event_type"] == CANCELLATION
    assert evs[0]["cancellation_code"] == "B" and evs[0]["tail_number"] is None
    assert evs[0]["airport"] == "ATL"


def test_desviado():
    evs = fila_a_eventos(fila(DIVERTED=1.0, ARR_DELAY=None, ACTUAL_ELAPSED_TIME=None), ZONAS)
    assert [e["event_type"] for e in evs] == [DEPARTURE, DIVERSION]


def test_salida_pasada_la_medianoche():
    # programado 23:50 local con 30 min de retraso -> sale a las 00:20 del día siguiente
    dep = fila_a_eventos(fila(CRS_DEP_TIME=2350, DEP_DELAY=30.0), ZONAS)[0]
    assert dep["event_ts"] == "2026-01-16T05:20:00Z"


def test_2400_es_medianoche_siguiente():
    assert hora_local_a_utc(date(2026, 1, 15), 2400, "America/New_York").isoformat() == "2026-01-16T05:00:00+00:00"


def test_fechas_en_dos_formatos():
    assert parsear_fecha("2026-01-15") == parsear_fecha("1/15/2026 12:00:00 AM") == date(2026, 1, 15)


def test_ids_deterministas_y_distintos_por_tipo():
    a, b = fila_a_eventos(fila(), ZONAS), fila_a_eventos(fila(), ZONAS)
    assert [e["event_id"] for e in a] == [e["event_id"] for e in b]
    assert a[0]["event_id"] != a[1]["event_id"]


def test_aeropuerto_sin_zona_se_omite():
    assert fila_a_eventos(fila(ORIGIN="XXX"), ZONAS) == []


def test_filas_de_pandas_con_nan():
    df = pd.DataFrame([fila(TAIL_NUM=None, CARRIER_DELAY=float("nan"), LATE_AIRCRAFT_DELAY=float("nan"))])
    evs = fila_a_eventos(df.to_dict("records")[0], ZONAS)
    assert evs[0]["tail_number"] is None and evs[1]["late_aircraft_delay"] is None
