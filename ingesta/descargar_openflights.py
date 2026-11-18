"""Descarga airports.dat y routes.dat de OpenFlights (licencia ODbL) a data/openflights/.

airports.dat aporta la zona horaria de cada aeropuerto (necesaria para pasar las horas
locales de BTS a UTC) y las coordenadas; routes.dat sirve para contrastar la red de rutas.
"""
import urllib.request
from pathlib import Path

BASE = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/"
DESTINO = Path("data/openflights")


def main():
    DESTINO.mkdir(parents=True, exist_ok=True)
    for nombre in ("airports.dat", "routes.dat"):
        ruta = DESTINO / nombre
        print(f"Descargando {nombre} ...")
        urllib.request.urlretrieve(BASE + nombre, ruta)
        print(f"  -> {ruta} ({ruta.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
