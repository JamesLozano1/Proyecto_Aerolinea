"""Zonas horarias por código IATA a partir de OpenFlights (airports.dat)."""
import csv

RUTA_POR_DEFECTO = "data/openflights/airports.dat"


def cargar_zonas(ruta: str = RUTA_POR_DEFECTO) -> dict:
    """Devuelve {IATA: 'America/New_York', ...}.

    Columnas de airports.dat: 0 id, 1 nombre, 2 ciudad, 3 país, 4 IATA, 5 ICAO,
    6 lat, 7 lon, 8 altitud, 9 offset, 10 DST, 11 zona tz database, 12 tipo, 13 fuente.
    """
    zonas = {}
    with open(ruta, encoding="utf-8", newline="") as f:
        for fila in csv.reader(f):
            if len(fila) < 12:
                continue
            iata, tz = fila[4], fila[11]
            if iata and iata != "\\N" and tz and tz != "\\N":
                zonas[iata] = tz
    return zonas
