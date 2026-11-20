// PageRank (ponderado por vuelos) e intermediación con Graph Data Science.
CALL gds.graph.drop('red', false) YIELD graphName RETURN graphName AS grafo_anterior_eliminado;

CALL gds.graph.project('red', 'Airport', {ROUTE: {properties: 'vuelos'}})
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount;

CALL gds.pageRank.write('red', {relationshipWeightProperty: 'vuelos', maxIterations: 50, writeProperty: 'pagerank'})
YIELD nodePropertiesWritten, ranIterations
RETURN 'pagerank' AS metrica, nodePropertiesWritten, ranIterations;

CALL gds.betweenness.write('red', {writeProperty: 'intermediacion'})
YIELD nodePropertiesWritten
RETURN 'intermediacion' AS metrica, nodePropertiesWritten;

// Componentes de la red completa (línea base para comparar con un cierre)
CALL gds.wcc.stats('red') YIELD componentCount, componentDistribution
RETURN componentCount AS componentes_red_completa, componentDistribution.max AS aeropuertos_en_mayor_componente;

// Top 15 por PageRank
MATCH (a:Airport)
RETURN a.iata AS aeropuerto, round(a.pagerank, 4) AS pagerank, round(a.intermediacion, 1) AS intermediacion,
       a.salidas AS salidas, a.retraso_salida_prom AS retraso_salida_prom
ORDER BY a.pagerank DESC LIMIT 15;

// Top 15 por intermediación
MATCH (a:Airport)
RETURN a.iata AS aeropuerto, round(a.intermediacion, 1) AS intermediacion, round(a.pagerank, 4) AS pagerank
ORDER BY a.intermediacion DESC LIMIT 15;
