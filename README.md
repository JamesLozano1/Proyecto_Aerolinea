# Reto 7 — Propagación de retrasos en la red de transporte aéreo

**Materia:** Bases de datos masivas · **Trabajo parcial:** Arquitecturas de datos masivos integradas

## Problema
Entender cómo el retraso de un aeropuerto se propaga a otros a través de las rotaciones de
aeronaves, e identificar aeropuertos críticos cuyo fallo desordena la red.

Preguntas que responde la solución:
1. ¿Qué proporción del retraso de un vuelo se hereda del vuelo anterior de la misma aeronave?
2. ¿Qué aeropuertos son más críticos (PageRank, intermediación)? ¿Qué pasa con la conectividad si se cierra uno?
3. ¿Cómo varía el retraso por aerolínea, franja horaria, mes y aeropuerto?

## Arquitectura
```
CSV BTS ──► productor Kafka ──► tópico flight-events ──► consumidor ──► Cassandra (3 tablas por consulta)
   │                                                                          
   ├──► Spark (herencia de retraso por aeronave; GraphFrames opcional)
   ├──► Neo4j (aeropuertos y rutas, PageRank, intermediación, cierre de aeropuerto)
   └──► S3 (Parquet) ──► BigQuery / Snowflake (retraso por aerolínea, franja, mes, aeropuerto)
```
| Componente | Por qué | Alternativa descartada |
|---|---|---|
| Kafka | Flujo ordenado de eventos; la clave = matrícula conserva el orden por aeronave | Escribir directo a Cassandra: sin desacople ni reproducción |
| Cassandra | Escritura masiva y consultas por clave conocida (aeropuerto+día, vuelo, matrícula) | PostgreSQL: más difícil de escalar horizontalmente para este patrón |
| Spark | Ventanas sobre millones de filas (`lag` por aeronave) | pandas: no escala |
| Neo4j | Centralidad y caminos alternativos sobre la red de rutas | Cassandra: sin recorridos de grafo |
| S3 + BigQuery/Snowflake | Histórico barato y análisis agregado en SQL | — |

## Estado
- [x] Fase 1: Kafka + Cassandra (ingesta, modelo, consultas)
- [ ] Fase 2: Spark — herencia de retraso
- [ ] Fase 3: Neo4j — grafo y centralidad
- [ ] Fase 4: S3 + BigQuery/Snowflake

## Requisitos
Docker Desktop, Python 3.11 (el driver de Cassandra aún da problemas en 3.12+), 8 GB de RAM libres.

## Datos
- **BTS On-Time Performance** (Reporting Carrier), enero–agosto 2026, ≈ 4–5 millones de vuelos. Dominio público (gobierno de EE. UU.).
  Descarga manual en transtats.bts.gov; guarda los CSV en `data/bts/2026/`.
- **OpenFlights** (`airports.dat`, `routes.dat`), licencia ODbL: zonas horarias y red de rutas.

Limpieza aplicada: horas locales `hhmm` → UTC (solo con la zona del origen; ver `ingesta/eventos.py`),
`2400` → medianoche, formato de fecha normalizado, columna vacía final del CSV ignorada,
cancelados sin matrícula conservados.

## Instalación y ejecución (Fase 1)
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                   # Windows: copy .env.example .env

docker compose up -d kafka cassandra
python -m ingesta.descargar_openflights
python -m pytest -q tests

# Esquema de Cassandra (esperar a que el contenedor esté healthy)
docker cp cassandra/schema.cql reto7-cassandra:/schema.cql
docker exec reto7-cassandra cqlsh -f /schema.cql

# Prueba pequeña (50 000 filas de enero)
python -m ingesta.productor --archivos "data/bts/2026/*.csv" --max-filas 50000
python -m ingesta.consumidor_cassandra --salir-al-terminar

# Consultas de ejemplo
docker cp cassandra/consultas.cql reto7-cassandra:/consultas.cql
docker exec reto7-cassandra cqlsh -f /consultas.cql
```
Para la carga completa quita `--max-filas`.

## Modelo de Cassandra (derivado de las consultas)
| Consulta | Tabla | Partición | Clustering |
|---|---|---|---|
| Aeropuerto + ventana de tiempo | `eventos_por_aeropuerto_dia` | `(airport, day)` | `event_ts DESC, event_id` |
| Número de vuelo | `eventos_por_vuelo` | `(carrier, flight_number)` | `event_ts DESC, event_id` |
| Matrícula (rotación) | `eventos_por_aeronave_mes` | `(tail_number, month)` | `event_ts ASC, event_id` |

Justificación completa de tamaños de partición en `cassandra/schema.cql`.

## Integrantes
_(completar)_
