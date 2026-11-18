"""Convierte una fila de BTS On-Time Performance en eventos de vuelo en UTC.

Por qué se trabaja así:
- BTS entrega horas locales en formato hhmm, sin zona horaria y sin fecha de llegada.
- Solo se necesita la zona horaria del ORIGEN: la salida real se calcula como
  programada + DEP_DELAY (así no hay problemas con salidas pasada la medianoche) y la
  llegada real como salida real + ACTUAL_ELAPSED_TIME (tiempo gate a gate en minutos).
- event_id es determinista (uuid5): reprocesar el mismo archivo no duplica filas en Cassandra.
"""
import math
import uuid
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

DEPARTURE = "DEPARTURE"
ARRIVAL = "ARRIVAL"
CANCELLATION = "CANCELLATION"
DIVERSION = "DIVERSION"

# Columnas del CSV de BTS que usa la ingesta
COLUMNAS = [
    "FL_DATE", "OP_UNIQUE_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_DELAY", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "DIVERTED",
    "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY",
    "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
]


@lru_cache(maxsize=None)
def _zona(nombre: str) -> ZoneInfo:
    return ZoneInfo(nombre)


def _num(valor):
    """float o None (NaN, vacío y None pasan a None)."""
    if valor is None:
        return None
    try:
        v = float(valor)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def _texto(valor):
    if valor is None:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    s = str(valor).strip()
    return s or None


def parsear_fecha(valor) -> date:
    """Acepta 2026-01-15 y 1/15/2026 12:00:00 AM (ambos aparecen en descargas de BTS)."""
    s = str(valor).strip()
    for formato in ("%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, formato).date()
        except ValueError:
            continue
    raise ValueError(f"Formato de fecha no reconocido: {valor!r}")


def hora_local_a_utc(fecha: date, hhmm: float, tz: str) -> datetime:
    """fecha + hhmm local -> datetime UTC. 2400 se interpreta como 00:00 del día siguiente."""
    hhmm = int(hhmm)
    naive = datetime.combine(fecha, time()) + timedelta(hours=hhmm // 100, minutes=hhmm % 100)
    return naive.replace(tzinfo=_zona(tz)).astimezone(timezone.utc)


def _iso(dt):
    return None if dt is None else dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _entero(valor):
    v = _num(valor)
    return None if v is None else int(round(v))


def fila_a_eventos(fila: dict, zonas: dict) -> list:
    """Devuelve 0, 1 o 2 eventos para una fila de BTS.

    - Cancelado:  CANCELLATION (en el origen, a la hora programada de salida)
    - Normal:     DEPARTURE (origen) + ARRIVAL (destino)
    - Desviado:   DEPARTURE + DIVERSION (la llegada real no está disponible en BTS)
    Devuelve [] si falta la zona horaria del origen o la hora programada de salida.
    """
    origen, destino = _texto(fila.get("ORIGIN")), _texto(fila.get("DEST"))
    tz = zonas.get(origen)
    crs_dep = _num(fila.get("CRS_DEP_TIME"))
    if tz is None or crs_dep is None or destino is None:
        return []

    fecha = parsear_fecha(fila["FL_DATE"])
    aerolinea = _texto(fila.get("OP_UNIQUE_CARRIER"))
    numero = _entero(fila.get("OP_CARRIER_FL_NUM"))
    matricula = _texto(fila.get("TAIL_NUM"))

    prog_dep = hora_local_a_utc(fecha, crs_dep, tz)
    crs_elapsed = _num(fila.get("CRS_ELAPSED_TIME"))
    prog_arr = prog_dep + timedelta(minutes=crs_elapsed) if crs_elapsed is not None else None

    base = {
        "flight_date": fecha.isoformat(),
        "carrier": aerolinea,
        "flight_number": numero,
        "tail_number": matricula,
        "origin": origen,
        "dest": destino,
        "sched_dep_utc": _iso(prog_dep),
        "sched_arr_utc": _iso(prog_arr),
        "distance": _entero(fila.get("DISTANCE")),
    }

    def crear(tipo, ts, aeropuerto, **extra):
        clave = f"{fecha}|{aerolinea}|{numero}|{origen}|{destino}|{int(crs_dep)}|{tipo}"
        evento = dict(base)
        evento.update({
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_OID, clave)),
            "event_type": tipo,
            "event_ts": _iso(ts),
            "airport": aeropuerto,
            "delay_minutes": None,
            "cancellation_code": None,
        })
        evento.update(extra)
        return evento

    if _num(fila.get("CANCELLED")) == 1:
        return [crear(CANCELLATION, prog_dep, origen,
                      cancellation_code=_texto(fila.get("CANCELLATION_CODE")))]

    dep_delay = _num(fila.get("DEP_DELAY"))
    dep_real = prog_dep + timedelta(minutes=dep_delay or 0)
    eventos = [crear(DEPARTURE, dep_real, origen, delay_minutes=_entero(dep_delay))]

    if _num(fila.get("DIVERTED")) == 1:
        eventos.append(crear(DIVERSION, prog_arr or dep_real, destino))
        return eventos

    elapsed = _num(fila.get("ACTUAL_ELAPSED_TIME"))
    arr_delay = _num(fila.get("ARR_DELAY"))
    if elapsed is not None:
        arr_real = dep_real + timedelta(minutes=elapsed)
    elif prog_arr is not None and arr_delay is not None:
        arr_real = prog_arr + timedelta(minutes=arr_delay)
    else:
        return eventos  # sin forma fiable de ubicar la llegada en el tiempo

    eventos.append(crear(
        ARRIVAL, arr_real, destino,
        delay_minutes=_entero(arr_delay),
        carrier_delay=_entero(fila.get("CARRIER_DELAY")),
        weather_delay=_entero(fila.get("WEATHER_DELAY")),
        nas_delay=_entero(fila.get("NAS_DELAY")),
        security_delay=_entero(fila.get("SECURITY_DELAY")),
        late_aircraft_delay=_entero(fila.get("LATE_AIRCRAFT_DELAY")),
    ))
    return eventos
