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
   └──► S3 (Parquet) ──► Athena (SQL sobre S3: retraso por aerolínea, franja, mes, aeropuerto)
```
| Componente | Por qué | Alternativa descartada |
|---|---|---|
| Kafka | Flujo ordenado de eventos; la clave = matrícula conserva el orden por aeronave | Escribir directo a Cassandra: sin desacople ni reproducción |
| Cassandra | Escritura masiva y consultas por clave conocida (aeropuerto+día, vuelo, matrícula) | PostgreSQL: más difícil de escalar horizontalmente para este patrón |
| Spark | Ventanas sobre millones de filas (`lag` por aeronave) | pandas: no escala |
| Neo4j | Centralidad y caminos alternativos sobre la red de rutas | Cassandra: sin recorridos de grafo |
| S3 + Athena | Histórico barato y análisis agregado en SQL sin mover datos | BigQuery/Snowflake: piden otra cuenta y copiar los datos |

## Estado
- [x] Fase 1: Kafka + Cassandra (ingesta, modelo, consultas)
- [x] Fase 2: Spark — herencia de retraso (`spark/herencia_retraso.py`)
- [~] Fase 3: grafo — prototipo en NetworkX (`grafo/`) y scripts de Neo4j + GDS (`neo4j/`), pendiente de ejecutar
- [~] Fase 4: S3 subido (`analitica/subir_s3.py`); análisis con Athena (`analitica/athena_analisis.sql`, sin probar)

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

## Fase 2 — Spark: herencia de retraso
```powershell
docker compose up -d spark-master spark-worker
docker exec reto7-spark-master /opt/spark/bin/spark-submit --master local[2] --driver-memory 2g /opt/app/spark/herencia_retraso.py --entrada "/data/bts/2026/ENERO.csv" --aeropuertos /data/openflights/airports.dat --salida /data/salida
```
Para los 8 meses usa `--entrada "/data/bts/2026/*.csv"`. Resultados en `data/salida/resumenes/` (CSV) y
`data/salida/vuelos_enriquecidos/` (Parquet particionado por mes, listo para subir a S3).

Método: se ordena cada matrícula por hora programada de salida en UTC y se une cada tramo con el anterior
(`lag`). Retraso heredado = min(retraso de salida, max(0, retraso de llegada anterior − holgura)), con
holgura = tiempo programado en tierra − 30 min. Se contrasta con `LateAircraftDelay` de BTS.
Limitación: el mínimo de 30 min en tierra es un supuesto (parámetro `--turnaround-minimo`).

## Fase 3 — Grafo de aeropuertos (sin Docker)
```powershell
pip install -r requirements.txt
python -m grafo.preparar_grafo --archivos "data/bts/2026/*.csv" --min-vuelos 100
python -m grafo.analisis_grafo --top 10
```
`--min-vuelos 100` descarta las rutas con menos de 100 vuelos operados en los 8 meses (unas 3 por semana); sin el filtro, rutas casi inexistentes cuentan igual que enlaces muy transitados. `preparar_grafo` resume los CSV en `data/grafo/nodos.csv` y `aristas.csv` (aeropuertos y rutas con vuelos y retraso
promedio). `analisis_grafo` calcula PageRank (ponderado por vuelos), intermediación, el efecto de cerrar cada uno de
los aeropuertos más importantes y las rutas alternativas sin la ruta directa. Salida en `data/salida/grafo/`.
Al interpretar el cierre, mira siempre `pares_perdidos`: los saltos promedio pueden bajar porque solo quedan los
pares cercanos que siguen conectados.
Estos resultados sirven para validar lo que luego calcule Neo4j con Graph Data Science.

### Neo4j + Graph Data Science
```powershell
docker compose up -d neo4j
docker logs reto7-neo4j --tail 20          # esperar el mensaje "Started."
docker exec reto7-neo4j cypher-shell -u neo4j -p reto7password -f /scripts/00_comprobar.cypher
docker exec reto7-neo4j cypher-shell -u neo4j -p reto7password --format plain -f /scripts/01_cargar.cypher
docker exec reto7-neo4j cypher-shell -u neo4j -p reto7password --format plain -f /scripts/02_centralidad.cypher
docker exec reto7-neo4j cypher-shell -u neo4j -p reto7password --format plain --param "cerrado => 'ATL'" -f /scripts/03_cierre.cypher
docker exec reto7-neo4j cypher-shell -u neo4j -p reto7password --format plain --param "origen => 'JFK'" --param "destino => 'LAX'" --param "cerrado => 'ORD'" --param "min_vuelos => 100" -f /scripts/04_alternativas.cypher
```
Requiere haber corrido antes `python -m grafo.preparar_grafo` (genera `data/grafo/nodos.csv` y `aristas.csv`).
Navegador de Neo4j: http://localhost:7474 (usuario `neo4j`).

## Fase 4 — Histórico en S3
1. Compacta el Parquet (Spark deja muchos archivos pequeños; esto no recalcula nada):
```powershell
docker exec reto7-spark-master /opt/spark/bin/spark-submit --master local[2] --driver-memory 2g /opt/app/spark/compactar_parquet.py --entrada /data/salida_8meses/vuelos_enriquecidos --salida /data/salida_8meses/vuelos_compacto
```
2. Credenciales de la cuenta educativa (AWS Academy: *AWS Details* → *AWS CLI: Show*). Son temporales: hay que
   repetirlo en cada sesión y **nunca** subirlas al repositorio ni compartirlas.
```powershell
$env:AWS_ACCESS_KEY_ID="..."; $env:AWS_SECRET_ACCESS_KEY="..."; $env:AWS_SESSION_TOKEN="..."; $env:AWS_DEFAULT_REGION="us-east-1"
```
3. Subir (el nombre del bucket debe ser único en todo AWS):
```powershell
python -m analitica.subir_s3 --bucket reto7-retrasos-TUNOMBRE-2026 --origen data/salida_8meses/vuelos_compacto --prefijo vuelos_enriquecidos --crear-bucket
python -m analitica.subir_s3 --bucket reto7-retrasos-TUNOMBRE-2026 --prefijo vuelos_enriquecidos --listar
```

### Análisis sobre S3 con Athena
Abre `analitica/athena_analisis.sql` en la consola de Athena (región us-east-2) y ejecútalo sentencia por sentencia.
Crea una tabla externa sobre los Parquet de S3 (sin copiar datos) y resuelve el análisis por aerolínea, franja
horaria, mes y aeropuerto. El reto menciona BigQuery o Snowflake como motor de análisis; usar Athena es una
alternativa que debe confirmarse con el docente.

## Modelo de Cassandra (derivado de las consultas)
| Consulta | Tabla | Partición | Clustering |
|---|---|---|---|
| Aeropuerto + ventana de tiempo | `eventos_por_aeropuerto_dia` | `(airport, day)` | `event_ts DESC, event_id` |
| Número de vuelo | `eventos_por_vuelo` | `(carrier, flight_number)` | `event_ts DESC, event_id` |
| Matrícula (rotación) | `eventos_por_aeronave_mes` | `(tail_number, month)` | `event_ts ASC, event_id` |

Justificación completa de tamaños de partición en `cassandra/schema.cql`.

## Integrantes
_(completar)_
