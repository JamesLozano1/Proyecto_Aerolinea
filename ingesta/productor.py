"""Lee los CSV de BTS y publica eventos de vuelo en Kafka.

Decisiones de diseño:
- Tópico `flight-events` con 6 particiones.
- Clave del mensaje = matrícula (TAIL_NUM). Kafka garantiza orden dentro de una partición,
  así todos los eventos de una misma aeronave quedan en orden: es lo que necesita el
  análisis de rotaciones. Los vuelos sin matrícula (cancelados) usan aerolínea+número.
- Los eventos se publican ordenados por event_ts dentro de cada día, simulando el flujo real.

Uso:
  python -m ingesta.productor --archivos "data/bts/2026/*.csv"
  python -m ingesta.productor --archivos "data/bts/2026/*January*.csv" --max-filas 50000
"""
import argparse
import glob
import json
import os
import sys
import time

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ingesta.aeropuertos import cargar_zonas
from ingesta.eventos import COLUMNAS, fila_a_eventos

TIPOS = {"TAIL_NUM": str, "CANCELLATION_CODE": str, "ORIGIN": str, "DEST": str,
         "OP_UNIQUE_CARRIER": str}


def crear_topico(bootstrap: str, topico: str, particiones: int):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topico in admin.list_topics(timeout=10).topics:
        return
    futuros = admin.create_topics([NewTopic(topico, num_partitions=particiones,
                                            replication_factor=1)])
    for f in futuros.values():
        f.result()
    print(f"Tópico {topico} creado con {particiones} particiones")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--archivos", required=True, help="patrón glob de los CSV de BTS")
    ap.add_argument("--topico", default="flight-events")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--zonas", default="data/openflights/airports.dat")
    ap.add_argument("--max-filas", type=int, default=None, help="límite de filas por archivo (pruebas)")
    ap.add_argument("--particiones", type=int, default=6)
    args = ap.parse_args()

    archivos = sorted(glob.glob(args.archivos))
    if not archivos:
        sys.exit(f"No se encontraron archivos con el patrón {args.archivos}")

    zonas = cargar_zonas(args.zonas)
    crear_topico(args.bootstrap, args.topico, args.particiones)
    productor = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50,
                          "compression.type": "lz4", "enable.idempotence": True})

    errores = []

    def al_entregar(err, _msg):
        if err is not None:
            errores.append(str(err))

    total_filas = total_eventos = filas_omitidas = 0
    inicio = time.time()

    for ruta in archivos:
        print(f"Leyendo {ruta} ...")
        df = pd.read_csv(ruta, usecols=COLUMNAS, dtype=TIPOS, index_col=False,
                         nrows=args.max_filas)
        for _fecha, dia in df.groupby("FL_DATE", sort=True):
            eventos = []
            for fila in dia.to_dict("records"):
                total_filas += 1
                evs = fila_a_eventos(fila, zonas)
                if not evs:
                    filas_omitidas += 1
                eventos.extend(evs)
            eventos.sort(key=lambda e: e["event_ts"])
            for e in eventos:
                clave = e["tail_number"] or f'{e["carrier"]}{e["flight_number"]}'
                valor = json.dumps(e, separators=(",", ":"))
                while True:
                    try:
                        productor.produce(args.topico, key=clave, value=valor,
                                          on_delivery=al_entregar)
                        break
                    except BufferError:
                        productor.poll(1)
                productor.poll(0)
            total_eventos += len(eventos)
        print(f"  acumulado: {total_filas:,} filas -> {total_eventos:,} eventos")

    productor.flush()
    seg = time.time() - inicio
    print(f"\nListo: {total_filas:,} filas, {total_eventos:,} eventos publicados en {seg:.1f} s "
          f"({total_eventos / max(seg, 1e-9):,.0f} eventos/s). Filas omitidas: {filas_omitidas:,}")
    if errores:
        print(f"ATENCIÓN: {len(errores)} mensajes con error de entrega. Ej.: {errores[0]}")


if __name__ == "__main__":
    main()
