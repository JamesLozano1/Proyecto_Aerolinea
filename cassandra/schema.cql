-- Modelo de Cassandra derivado de las consultas (query-first).
-- Un mismo evento se escribe en 3 tablas (desnormalización), una por patrón de consulta.
-- Todas las tablas guardan casi las mismas columnas; solo cambia la clave primaria.

CREATE KEYSPACE IF NOT EXISTS retrasos
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE retrasos;

-- Q1: eventos de un aeropuerto en una ventana de tiempo.
-- Partición: (airport, day). Un aeropuerto grande (ATL, ORD) genera ~2 000-3 000 eventos
-- por día, así que la partición queda acotada (nunca crece sin límite) y la ventana
-- de tiempo se resuelve leyendo 1 partición por día con un rango sobre event_ts.
-- Clustering: event_ts DESC (lo más reciente primero); event_id desempata eventos simultáneos.
CREATE TABLE IF NOT EXISTS eventos_por_aeropuerto_dia (
  airport text,
  day date,
  event_ts timestamp,
  event_id uuid,
  event_type text,
  carrier text,
  flight_number int,
  tail_number text,
  month text,
  origin text,
  dest text,
  delay_minutes int,
  cancellation_code text,
  flight_date date,
  PRIMARY KEY ((airport, day), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts DESC, event_id ASC);

-- Q2: historial de un número de vuelo (aerolínea + número).
-- Partición: (carrier, flight_number). Un vuelo opera 1-2 veces al día, es decir
-- ~2-4 eventos/día, cerca de 1 500 filas por año: partición pequeña y acotada.
CREATE TABLE IF NOT EXISTS eventos_por_vuelo (
  carrier text,
  flight_number int,
  event_ts timestamp,
  event_id uuid,
  event_type text,
  tail_number text,
  airport text,
  day date,
  month text,
  origin text,
  dest text,
  delay_minutes int,
  cancellation_code text,
  flight_date date,
  PRIMARY KEY ((carrier, flight_number), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts DESC, event_id ASC);

-- Q3: rotación de una aeronave (matrícula) en orden cronológico.
-- Partición: (tail_number, month). Una aeronave hace ~5-6 tramos/día, ~12 eventos/día,
-- unas 360 filas por mes. Sin el bucket "month" la partición crecería indefinidamente.
-- Clustering ASC por event_ts: recorre la cadena de vuelos para medir la herencia de retraso.
CREATE TABLE IF NOT EXISTS eventos_por_aeronave_mes (
  tail_number text,
  month text,
  event_ts timestamp,
  event_id uuid,
  event_type text,
  carrier text,
  flight_number int,
  airport text,
  day date,
  origin text,
  dest text,
  delay_minutes int,
  cancellation_code text,
  flight_date date,
  PRIMARY KEY ((tail_number, month), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts ASC, event_id ASC);
