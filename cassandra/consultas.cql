USE retrasos;

-- Q1: salidas/llegadas en ATL el 2026-01-15 entre las 12:00Z y las 14:00Z
SELECT event_ts, event_type, carrier, flight_number, tail_number, delay_minutes
FROM eventos_por_aeropuerto_dia
WHERE airport = 'ATL' AND day = '2026-01-15'
  AND event_ts >= '2026-01-15 12:00:00+0000' AND event_ts < '2026-01-15 14:00:00+0000';

-- Q2: historial del vuelo DL 1234
SELECT event_ts, event_type, origin, dest, delay_minutes
FROM eventos_por_vuelo
WHERE carrier = 'DL' AND flight_number = 1234 LIMIT 20;

-- Q3: rotación de la aeronave N123DL en enero de 2026 (orden cronológico)
SELECT event_ts, event_type, origin, dest, delay_minutes
FROM eventos_por_aeronave_mes
WHERE tail_number = 'N123DL' AND month = '2026-01';
