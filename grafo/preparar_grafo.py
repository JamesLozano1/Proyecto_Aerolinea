"""Agrega los CSV de BTS en una red de aeropuertos (nodos) y rutas dirigidas (aristas).

Salida (data/grafo/):
  nodos.csv   iata, ciudad, nombre, lat, lon, salidas, llegadas, retraso_salida_prom
  aristas.csv origen, destino, vuelos, cancelados, desviados, retraso_salida_prom, retraso_llegada_prom, distancia

Se lee por bloques para no cargar millones de filas en memoria. No requiere Docker.
Uso: python -m grafo.preparar_grafo --archivos "data/bts/2026/*.csv"
"""
import argparse
import csv
import glob
import sys
from pathlib import Path

import numpy as np
import pandas as pd

COLS = ["ORIGIN", "DEST", "ORIGIN_CITY_NAME", "DEP_DELAY_NEW", "ARR_DELAY_NEW", "CANCELLED",
        "DIVERTED", "DISTANCE"]


def leer_openflights(ruta):
    info = {}
    with open(ruta, encoding="utf-8", newline="") as f:
        for fila in csv.reader(f):
            if len(fila) > 7 and fila[4] and fila[4] != "\\N":
                info[fila[4]] = (fila[1], float(fila[6]), float(fila[7]))
    return info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--archivos", required=True)
    ap.add_argument("--openflights", default="data/openflights/airports.dat")
    ap.add_argument("--salida", default="data/grafo")
    args = ap.parse_args()

    archivos = sorted(glob.glob(args.archivos))
    if not archivos:
        sys.exit(f"No hay archivos con el patrón {args.archivos}")

    partes, ciudades = [], {}
    for ruta in archivos:
        print(f"Leyendo {ruta} ...")
        for bloque in pd.read_csv(ruta, usecols=COLS, chunksize=500_000, index_col=False,
                                  dtype={"ORIGIN": str, "DEST": str, "ORIGIN_CITY_NAME": str}):
            bloque["con_dep"] = bloque["DEP_DELAY_NEW"].notna()
            bloque["con_arr"] = bloque["ARR_DELAY_NEW"].notna()
            partes.append(bloque.groupby(["ORIGIN", "DEST"]).agg(
                vuelos=("CANCELLED", "size"), cancelados=("CANCELLED", "sum"),
                desviados=("DIVERTED", "sum"), dep_sum=("DEP_DELAY_NEW", "sum"),
                dep_n=("con_dep", "sum"), arr_sum=("ARR_DELAY_NEW", "sum"),
                arr_n=("con_arr", "sum"), distancia=("DISTANCE", "max")).reset_index())
            ciudades.update(bloque.drop_duplicates("ORIGIN").set_index("ORIGIN")["ORIGIN_CITY_NAME"].to_dict())

    a = pd.concat(partes).groupby(["ORIGIN", "DEST"], as_index=False).agg(
        vuelos=("vuelos", "sum"), cancelados=("cancelados", "sum"), desviados=("desviados", "sum"),
        dep_sum=("dep_sum", "sum"), dep_n=("dep_n", "sum"), arr_sum=("arr_sum", "sum"),
        arr_n=("arr_n", "sum"), distancia=("distancia", "max"))
    a = a[a["ORIGIN"] != a["DEST"]]
    a["retraso_salida_prom"] = (a["dep_sum"] / a["dep_n"].replace(0, np.nan)).round(2)
    a["retraso_llegada_prom"] = (a["arr_sum"] / a["arr_n"].replace(0, np.nan)).round(2)

    por_origen = a.groupby("ORIGIN").agg(salidas=("vuelos", "sum"), dep_sum=("dep_sum", "sum"), dep_n=("dep_n", "sum"))
    por_destino = a.groupby("DEST").agg(llegadas=("vuelos", "sum"))
    nodos = pd.DataFrame({"iata": sorted(set(a["ORIGIN"]) | set(a["DEST"]))}).set_index("iata")
    nodos = nodos.join(por_origen[["salidas"]]).join(por_destino).fillna({"salidas": 0, "llegadas": 0})
    nodos["retraso_salida_prom"] = (por_origen["dep_sum"] / por_origen["dep_n"].replace(0, np.nan)).round(2)
    nodos["ciudad"] = nodos.index.map(ciudades.get)

    try:
        of = leer_openflights(args.openflights)
    except FileNotFoundError:
        of = {}
        print("Aviso: no se encontró airports.dat; los nodos quedarán sin coordenadas")
    nodos["nombre"] = [of.get(i, (None,))[0] for i in nodos.index]
    nodos["lat"] = [of[i][1] if i in of else np.nan for i in nodos.index]
    nodos["lon"] = [of[i][2] if i in of else np.nan for i in nodos.index]

    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)
    nodos.reset_index()[["iata", "ciudad", "nombre", "lat", "lon", "salidas", "llegadas",
                         "retraso_salida_prom"]].to_csv(salida / "nodos.csv", index=False)
    a.rename(columns={"ORIGIN": "origen", "DEST": "destino"})[
        ["origen", "destino", "vuelos", "cancelados", "desviados", "retraso_salida_prom",
         "retraso_llegada_prom", "distancia"]].to_csv(salida / "aristas.csv", index=False)
    print(f"Listo: {len(nodos)} aeropuertos y {len(a)} rutas dirigidas en {salida}/")


if __name__ == "__main__":
    main()
