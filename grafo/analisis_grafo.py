"""Análisis de la red de aeropuertos: centralidad, cierre de aeropuertos y rutas alternativas.

Se calcula con NetworkX (no requiere Docker) y sirve para validar los resultados que luego se
obtienen en Neo4j con Graph Data Science. Entrada: data/grafo/nodos.csv y aristas.csv.

- PageRank ponderado por número de vuelos: importancia de un aeropuerto según el tráfico que recibe
  de otros aeropuertos importantes.
- Intermediación (betweenness) sobre saltos: cuántos caminos mínimos entre otros pares pasan por él.
- Cierre: se elimina el aeropuerto y se mide cuántos pares origen-destino dejan de ser alcanzables,
  cuánto aumentan los saltos promedio y qué porción del tráfico desaparece.
- Rutas alternativas: caminos más cortos entre dos aeropuertos sin usar la ruta directa.
"""
import argparse
from pathlib import Path

import networkx as nx
import pandas as pd


def cargar(directorio):
    nodos = pd.read_csv(Path(directorio) / "nodos.csv")
    aristas = pd.read_csv(Path(directorio) / "aristas.csv")
    G = nx.DiGraph()
    for r in nodos.itertuples():
        G.add_node(r.iata, retraso=r.retraso_salida_prom)
    for r in aristas.itertuples():
        G.add_edge(r.origen, r.destino, vuelos=int(r.vuelos), retraso=r.retraso_salida_prom)
    return G


def pares_y_saltos(G, excluir=None):
    """(# pares ordenados alcanzables, suma de saltos) ignorando el nodo `excluir` como extremo."""
    pares = saltos = 0
    for origen, destinos in nx.all_pairs_shortest_path_length(G):
        if origen == excluir:
            continue
        for destino, d in destinos.items():
            if destino != origen and destino != excluir:
                pares += 1
                saltos += d
    return pares, saltos


def efecto_cierre(G, nodo):
    antes_pares, antes_saltos = pares_y_saltos(G, excluir=nodo)
    H = G.copy()
    trafico_nodo = sum(d["vuelos"] for _, _, d in G.in_edges(nodo, data=True)) + \
        sum(d["vuelos"] for _, _, d in G.out_edges(nodo, data=True))
    H.remove_node(nodo)
    des_pares, des_saltos = pares_y_saltos(H)
    total_vuelos = sum(d["vuelos"] for _, _, d in G.edges(data=True))
    aislados = [n for n in H if H.degree(n) == 0 and G.degree(n) > 0]
    mayor = max((len(c) for c in nx.weakly_connected_components(H)), default=0)
    return {
        "aeropuerto_cerrado": nodo,
        "pares_alcanzables_antes": antes_pares,
        "pares_alcanzables_despues": des_pares,
        "pares_perdidos": antes_pares - des_pares,
        "pct_pares_perdidos": round(100 * (antes_pares - des_pares) / antes_pares, 3) if antes_pares else 0,
        "saltos_prom_antes": round(antes_saltos / antes_pares, 3) if antes_pares else None,
        "saltos_prom_despues": round(des_saltos / des_pares, 3) if des_pares else None,
        "pct_trafico_afectado": round(100 * trafico_nodo / total_vuelos, 2),
        "aeropuertos_aislados": len(aislados),
        "pct_en_mayor_componente": round(100 * mayor / H.number_of_nodes(), 2),
    }


def rutas_alternativas(G, origen, destino, k=3):
    H = G.copy()
    if H.has_edge(origen, destino):
        H.remove_edge(origen, destino)
    if origen not in H or destino not in H or not nx.has_path(H, origen, destino):
        return []
    rutas = []
    for camino in nx.shortest_simple_paths(H, origen, destino):
        rutas.append(camino)
        if len(rutas) == k:
            break
    return rutas


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", default="data/grafo")
    ap.add_argument("--salida", default="data/salida/grafo")
    ap.add_argument("--top", type=int, default=10, help="aeropuertos a cerrar (los de mayor PageRank)")
    ap.add_argument("--par", nargs=2, metavar=("ORIGEN", "DESTINO"), help="par para rutas alternativas")
    args = ap.parse_args()

    G = cargar(args.entrada)
    print(f"Red: {G.number_of_nodes()} aeropuertos, {G.number_of_edges()} rutas dirigidas")

    pr = nx.pagerank(G, weight="vuelos")
    bt = nx.betweenness_centrality(G, normalized=True)
    cent = pd.DataFrame({
        "iata": list(G.nodes),
        "pagerank": [pr[n] for n in G], "intermediacion": [bt[n] for n in G],
        "grado_salida": [G.out_degree(n) for n in G], "grado_entrada": [G.in_degree(n) for n in G],
        "vuelos": [sum(d["vuelos"] for _, _, d in G.out_edges(n, data=True)) for n in G],
        "retraso_salida_prom": [G.nodes[n]["retraso"] for n in G],
    }).sort_values("pagerank", ascending=False)
    print("\n=== CENTRALIDAD (top 15 por PageRank) ===")
    print(cent.head(15).to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    cierres = pd.DataFrame([efecto_cierre(G, n) for n in cent["iata"].head(args.top)])
    print(f"\n=== CIERRE DE AEROPUERTO (top {args.top} por PageRank) ===")
    print(cierres.to_string(index=False))

    if args.par:
        pares = [tuple(args.par)]
    else:  # las 5 rutas con más vuelos
        pares = [(o, d) for o, d, _ in sorted(G.edges(data="vuelos"), key=lambda e: -e[2])[:5]]
    alt = []
    print("\n=== RUTAS ALTERNATIVAS (sin la ruta directa) ===")
    for o, d in pares:
        rutas = rutas_alternativas(G, o, d)
        for i, r in enumerate(rutas, 1):
            alt.append({"origen": o, "destino": d, "alternativa": i, "saltos": len(r) - 1, "ruta": " > ".join(r)})
            print(f"{o}->{d} alt {i} ({len(r) - 1} saltos): {' > '.join(r)}")
        if not rutas:
            print(f"{o}->{d}: sin alternativa")

    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)
    cent.to_csv(salida / "centralidad.csv", index=False)
    cierres.to_csv(salida / "cierres.csv", index=False)
    pd.DataFrame(alt).to_csv(salida / "rutas_alternativas.csv", index=False)
    print(f"\nResultados guardados en {salida}/")


if __name__ == "__main__":
    main()
