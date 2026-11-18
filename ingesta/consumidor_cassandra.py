"""Consume `flight-events` de Kafka y persiste cada evento en las 3 tablas de Cassandra.

- Escritura idempotente: event_id es determinista, así que un reproceso (at-least-once)
  sobrescribe la misma fila en lugar de duplicarla.
- Los offsets se confirman SOLO después de que Cassandra aceptó el lote.

Uso:  python -m ingesta.consumidor_cassandra
"""
import argparse
import json
import os
import time
import uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from confluent_kafka import Consumer, KafkaError

COLUMNAS = ["airport", "day", "tail_number", "month", "carrier", "flight_number", "event_ts",
            "event_id", "event_type", "origin", "dest", "delay_minutes", "cancellation_code",
            "flight_date"]

# Columnas que existen en cada tabla (la clave primaria varía; ver cassandra/schema.cql)
TABLAS = {
    "eventos_por_aeropuerto_dia": ["airport", "day", "event_ts", "event_id", "event_type", "carrier",
                                   "flight_number", "tail_number", "month", "origin", "dest",
                                   "delay_minutes", "cancellation_code", "flight_date"],
    "eventos_por_vuelo": ["carrier", "flight_number", "event_ts", "event_id", "event_type",
                          "tail_number", "airport", "day", "month", "origin", "dest",
                          "delay_minutes", "cancellation_code", "flight_date"],
    "eventos_por_aeronave_mes": ["tail_number", "month", "event_ts", "event_id", "event_type",
                                 "carrier", "flight_number", "airport", "day", "origin", "dest",
                                 "delay_minutes", "cancellation_code", "flight_date"],
}


def evento_a_registro(e: dict) -> dict:
    ts = datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00"))
    return {
        "airport": e["airport"], "day": ts.date(), "tail_number": e["tail_number"],
        "month": ts.strftime("%Y-%m"), "carrier": e["carrier"],
        "flight_number": e["flight_number"], "event_ts": ts,
        "event_id": uuid.UUID(e["event_id"]), "event_type": e["event_type"],
        "origin": e["origin"], "dest": e["dest"], "delay_minutes": e["delay_minutes"],
        "cancellation_code": e["cancellation_code"],
        "flight_date": datetime.fromisoformat(e["flight_date"]).date(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topico", default="flight-events")
    ap.add_argument("--grupo", default="cassandra-writer")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--cassandra", default=os.environ.get("CASSANDRA_HOST", "localhost"))
    ap.add_argument("--lote", type=int, default=500)
    ap.add_argument("--salir-al-terminar", action="store_true",
                    help="termina cuando no llegan mensajes por 10 s (útil en pruebas)")
    args = ap.parse_args()

    sesion = Cluster([args.cassandra]).connect("retrasos")
    sentencias = {
        t: sesion.prepare(f"INSERT INTO {t} ({', '.join(c)}) VALUES ({', '.join('?' * len(c))})")
        for t, c in TABLAS.items()
    }

    consumidor = Consumer({"bootstrap.servers": args.bootstrap, "group.id": args.grupo,
                           "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumidor.subscribe([args.topico])

    escritos = 0
    inicio = time.time()
    ultimo_mensaje = time.time()
    try:
        while True:
            mensajes = consumidor.consume(num_messages=args.lote, timeout=1.0)
            if not mensajes:
                if args.salir_al_terminar and time.time() - ultimo_mensaje > 10:
                    break
                continue
            ultimo_mensaje = time.time()

            registros = []
            for m in mensajes:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print("Error de Kafka:", m.error())
                    continue
                registros.append(evento_a_registro(json.loads(m.value())))

            for tabla, cols in TABLAS.items():
                filas = registros
                if tabla == "eventos_por_aeronave_mes":
                    filas = [r for r in registros if r["tail_number"]]  # clave no puede ser nula
                params = [tuple(r[c] for c in cols) for r in filas]
                resultados = execute_concurrent_with_args(sesion, sentencias[tabla], params,
                                                          concurrency=64, raise_on_first_error=False)
                fallos = [r[1] for r in resultados if not r[0]]
                if fallos:
                    raise RuntimeError(f"{len(fallos)} escrituras fallaron en {tabla}: {fallos[0]}")

            consumidor.commit(asynchronous=False)
            escritos += len(registros)
            if escritos % 10000 < args.lote:
                seg = time.time() - inicio
                print(f"{escritos:,} eventos escritos ({escritos / max(seg, 1e-9):,.0f} eventos/s)")
    except KeyboardInterrupt:
        pass
    finally:
        consumidor.close()
        print(f"Total escrito: {escritos:,} eventos en {time.time() - inicio:.1f} s")


if __name__ == "__main__":
    main()
