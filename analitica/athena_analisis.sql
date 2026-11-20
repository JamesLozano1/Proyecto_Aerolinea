-- Reto 7 - Fase 4: análisis del histórico directamente sobre S3 con Amazon Athena (SQL).
-- Ejecutar en la consola de Athena (región us-east-2, Ohio), una sentencia a la vez.
-- SIN PROBAR contra una cuenta real: si algo falla, copia el mensaje de error.
--
-- Antes de empezar: Athena > Query editor > Settings > Manage > "Query result location":
--   s3://reto7-retrasos-jamesreto7-2026/athena-resultados/

------------------------------------------------------------------------------------------
-- 1. Base de datos y tabla externa (los datos NO se copian: Athena lee los Parquet de S3)
------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS reto7;

CREATE EXTERNAL TABLE IF NOT EXISTS reto7.vuelos (
  fecha date,
  op_unique_carrier string,
  tail_num string,
  op_carrier_fl_num double,
  origin string,
  dest string,
  hora_salida int,
  tramo_del_dia int,
  dep_delay_new double,
  arr_delay_new double,
  arr_del15 double,
  late_aircraft_delay double,
  vinculo boolean,
  gap_tierra_min double,
  heredado double
)
PARTITIONED BY (mes int)
STORED AS PARQUET
LOCATION 's3://reto7-retrasos-jamesreto7-2026/vuelos_enriquecidos/';

-- Registra las particiones mes=1 ... mes=8 (carpetas de S3)
MSCK REPAIR TABLE reto7.vuelos;

SELECT mes, count(*) AS vuelos FROM reto7.vuelos GROUP BY mes ORDER BY mes;   -- 8 filas, suma = 4 620 415

------------------------------------------------------------------------------------------
-- 2. Análisis pedido en el reto
------------------------------------------------------------------------------------------
-- 2.1 Retraso por aerolínea
SELECT op_unique_carrier AS aerolinea, count(*) AS vuelos,
       round(avg(dep_delay_new), 2) AS retraso_salida_prom_min,
       round(avg(arr_delay_new), 2) AS retraso_llegada_prom_min,
       round(100.0 * count_if(arr_del15 = 1) / nullif(count(arr_del15), 0), 2) AS pct_llegadas_tardias
FROM reto7.vuelos
GROUP BY 1
ORDER BY retraso_llegada_prom_min DESC;

-- 2.2 Retraso por franja horaria
SELECT CASE WHEN hora_salida BETWEEN 0 AND 5 THEN '1 Madrugada (0-5)'
            WHEN hora_salida BETWEEN 6 AND 11 THEN '2 Manana (6-11)'
            WHEN hora_salida BETWEEN 12 AND 17 THEN '3 Tarde (12-17)'
            ELSE '4 Noche (18-23)' END AS franja,
       count(*) AS vuelos,
       round(avg(dep_delay_new), 2) AS retraso_salida_prom_min,
       round(avg(arr_delay_new), 2) AS retraso_llegada_prom_min
FROM reto7.vuelos
GROUP BY 1
ORDER BY 1;

-- 2.3 Retraso por mes
SELECT mes, count(*) AS vuelos,
       round(avg(dep_delay_new), 2) AS retraso_salida_prom_min,
       round(avg(arr_delay_new), 2) AS retraso_llegada_prom_min
FROM reto7.vuelos
GROUP BY mes
ORDER BY mes;

-- 2.4 Retraso por aeropuerto de origen (solo aeropuertos con volumen suficiente)
SELECT origin AS aeropuerto, count(*) AS vuelos,
       round(avg(dep_delay_new), 2) AS retraso_salida_prom_min,
       round(100.0 * sum(heredado) / nullif(sum(dep_delay_new), 0), 2) AS pct_heredado
FROM reto7.vuelos
GROUP BY origin
HAVING count(*) >= 20000
ORDER BY retraso_salida_prom_min DESC
LIMIT 20;

-- 2.5 Herencia del retraso según el tramo del día (contrasta con el análisis de Spark)
SELECT tramo_del_dia, count(*) AS vuelos,
       round(avg(dep_delay_new), 2) AS retraso_salida_prom_min,
       round(100.0 * sum(heredado) / nullif(sum(dep_delay_new), 0), 2) AS pct_heredado
FROM reto7.vuelos
WHERE tramo_del_dia <= 8
GROUP BY tramo_del_dia
ORDER BY tramo_del_dia;
-- Nota: `heredado` se calculó en Spark con 30 min mínimos en tierra (el valor central del análisis).

-- 2.6 Quién genera más retraso propio (minutos de retraso de salida que NO vienen del vuelo anterior)
SELECT origin AS aeropuerto, count(*) AS vuelos,
       round(sum(dep_delay_new - heredado) / 1000.0, 1) AS miles_min_generados_localmente,
       round(sum(heredado) / 1000.0, 1) AS miles_min_heredados,
       round(100.0 * sum(dep_delay_new - heredado) / nullif(sum(dep_delay_new), 0), 1) AS pct_generado_localmente
FROM reto7.vuelos
GROUP BY origin
HAVING count(*) >= 20000
ORDER BY sum(dep_delay_new - heredado) DESC
LIMIT 15;

-- 2.7 Mediana y percentil 90 del retraso de llegada por aerolínea (el promedio esconde la cola larga)
SELECT op_unique_carrier AS aerolinea, count(*) AS vuelos,
       round(approx_percentile(arr_delay_new, 0.5), 1) AS mediana_llegada_min,
       round(approx_percentile(arr_delay_new, 0.9), 1) AS p90_llegada_min
FROM reto7.vuelos
GROUP BY op_unique_carrier
ORDER BY p90_llegada_min DESC;
