// Carga la red de aeropuertos (nodos.csv, aristas.csv generados por grafo/preparar_grafo.py)
CREATE CONSTRAINT airport_iata IF NOT EXISTS FOR (a:Airport) REQUIRE a.iata IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///nodos.csv' AS row
MERGE (a:Airport {iata: row.iata})
SET a.ciudad = row.ciudad,
    a.nombre = row.nombre,
    a.lat = CASE row.lat WHEN '' THEN null ELSE toFloat(row.lat) END,
    a.lon = CASE row.lon WHEN '' THEN null ELSE toFloat(row.lon) END,
    a.salidas = toInteger(toFloat(row.salidas)),
    a.llegadas = toInteger(toFloat(row.llegadas)),
    a.retraso_salida_prom = CASE row.retraso_salida_prom WHEN '' THEN null ELSE toFloat(row.retraso_salida_prom) END;

LOAD CSV WITH HEADERS FROM 'file:///aristas.csv' AS row
MATCH (o:Airport {iata: row.origen}), (d:Airport {iata: row.destino})
MERGE (o)-[r:ROUTE]->(d)
SET r.vuelos = toInteger(toFloat(row.vuelos)),
    r.cancelados = toInteger(toFloat(row.cancelados)),
    r.retraso_salida_prom = CASE row.retraso_salida_prom WHEN '' THEN null ELSE toFloat(row.retraso_salida_prom) END,
    r.distancia = toInteger(toFloat(row.distancia));

// Verificación
MATCH (a:Airport) WITH count(a) AS aeropuertos
MATCH ()-[r:ROUTE]->() RETURN aeropuertos, count(r) AS rutas;
