// Efecto de "cerrar" un aeropuerto. Parámetro: $cerrado (código IATA)
// Ejemplo: cypher-shell ... --param "cerrado => 'ATL'" -f /scripts/03_cierre.cypher

// Tráfico (vuelos programados) que desaparece
MATCH (a:Airport {iata: $cerrado})-[r:ROUTE]-()
WITH sum(r.vuelos) AS afectado
MATCH ()-[t:ROUTE]->()
WITH afectado, sum(t.vuelos) AS total
RETURN $cerrado AS cerrado, afectado AS vuelos_afectados, total AS vuelos_totales,
       round(100.0 * afectado / total, 2) AS pct_trafico_afectado;

// Aeropuertos que quedan sin ninguna ruta
MATCH (a:Airport)
WHERE a.iata <> $cerrado AND NOT EXISTS { MATCH (a)-[:ROUTE]-(b:Airport) WHERE b.iata <> $cerrado }
RETURN count(a) AS aeropuertos_aislados;

// Red sin el aeropuerto cerrado y sus componentes conexos
CALL gds.graph.drop('sin_cierre', false) YIELD graphName RETURN graphName AS grafo_anterior_eliminado;

MATCH (s:Airport)-[r:ROUTE]->(t:Airport)
WHERE s.iata <> $cerrado AND t.iata <> $cerrado
WITH gds.graph.project('sin_cierre', s, t, {relationshipProperties: r {.vuelos}}) AS g
RETURN g.graphName AS grafo, g.nodeCount AS aeropuertos_conectados, g.relationshipCount AS rutas;

CALL gds.wcc.stats('sin_cierre') YIELD componentCount, componentDistribution
RETURN componentCount AS componentes, componentDistribution.max AS aeropuertos_en_mayor_componente;
