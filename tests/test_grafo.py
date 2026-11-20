import networkx as nx

from grafo.analisis_grafo import efecto_cierre, rutas_alternativas


def red():
    G = nx.DiGraph()
    # Ruta directa A->B, y dos alternativas de 2 saltos: por X (mucho tráfico) y por Y (casi nada)
    for o, d, v in [("A", "B", 500), ("A", "Y", 2), ("Y", "B", 3), ("A", "X", 400), ("X", "B", 300),
                    ("B", "A", 500), ("X", "A", 400), ("B", "X", 300), ("Y", "A", 2), ("B", "Y", 3)]:
        G.add_edge(o, d, vuelos=v)
    return G


def test_alternativas_ordenadas_por_capacidad_y_sin_ruta_directa():
    rutas = rutas_alternativas(red(), "A", "B", k=3)
    assert rutas[0] == (["A", "X", "B"], 300)   # la de mayor capacidad primero
    assert rutas[1] == (["A", "Y", "B"], 2)
    assert all(len(c) > 2 for c, _ in rutas)    # ninguna es la ruta directa


def test_cierre_cuenta_aeropuertos_aislados():
    G = nx.DiGraph()
    for o, d in [("H", "A"), ("A", "H"), ("H", "B"), ("B", "H"), ("H", "C"), ("C", "H")]:
        G.add_edge(o, d, vuelos=10)
    r = efecto_cierre(G, "H")
    assert r["aeropuertos_aislados"] == 3 and r["pares_alcanzables_despues"] == 0
