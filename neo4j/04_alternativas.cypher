// Rutas alternativas entre dos aeropuertos SIN la ruta directa (de 2 a 3 saltos).
// Parámetros: $origen, $destino, $cerrado (aeropuerto a excluir de las escalas; usa 'NINGUNO' si no aplica)
// y $min_vuelos (capacidad mínima de cada tramo, por ejemplo 100).
// Ejemplo: --param "origen => 'JFK'" --param "destino => 'LAX'" --param "cerrado => 'ORD'" --param "min_vuelos => 100"
MATCH (a:Airport {iata: $origen}), (b:Airport {iata: $destino})
MATCH p = (a)-[:ROUTE*2..3]->(b)
WHERE NONE(n IN nodes(p) WHERE n.iata = $cerrado) AND ALL(r IN relationships(p) WHERE r.vuelos >= $min_vuelos)
WITH p, reduce(minimo = 1000000, r IN relationships(p) | CASE WHEN r.vuelos < minimo THEN r.vuelos ELSE minimo END) AS vuelos_tramo_debil
RETURN [n IN nodes(p) | n.iata] AS ruta, length(p) AS saltos, vuelos_tramo_debil
ORDER BY saltos, vuelos_tramo_debil DESC
LIMIT 10;
