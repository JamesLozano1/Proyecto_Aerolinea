"""Interfaz de demostración del Reto 7. Ejecutar desde la raíz del repo:
    pip install -r demo/requirements-demo.txt
    python demo/app.py        ->  http://localhost:5000
Las credenciales de AWS se toman de las variables de entorno de la misma terminal."""
import json, os, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from flask import Flask, Response, jsonify, request, send_from_directory

RAIZ = Path(__file__).resolve().parent.parent
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CASS = os.environ.get("CASSANDRA_HOST", "localhost")
NEO = (os.environ.get("NEO4J_URI", "bolt://localhost:7687"), "neo4j", os.environ.get("NEO4J_PASSWORD", "reto7password"))
BUCKET = os.environ.get("S3_BUCKET", "reto7-retrasos-jamesreto7-2026")
REGION = os.environ.get("ATHENA_REGION", "us-east-2")
SALIDA = os.environ.get("ATHENA_OUTPUT", f"s3://{BUCKET}/athena-resultados/")
TOPICO = "flight-events"
app = Flask(__name__)
_cass = _neo = None


def cass():
    global _cass
    if _cass is None:
        from cassandra.cluster import Cluster
        _cass = Cluster([CASS]).connect("retrasos")
    return _cass


def neo():
    global _neo
    if _neo is None:
        from neo4j import GraphDatabase
        _neo = GraphDatabase.driver(NEO[0], auth=NEO[1:])
    return _neo


@app.errorhandler(Exception)
def error(e):
    return jsonify(error=f"{type(e).__name__}: {e}"), 500


@app.get("/")
def inicio():
    return send_from_directory(Path(__file__).parent, "index.html")


# ---------- Estado de servicios ----------
def chk_kafka():
    from confluent_kafka.admin import AdminClient
    t = AdminClient({"bootstrap.servers": KAFKA}).list_topics(timeout=5).topics.get(TOPICO)
    if not t:
        raise RuntimeError(f"responde, pero falta el tópico {TOPICO}")
    return f"tópico {TOPICO}, {len(t.partitions)} particiones"


def chk_cass():
    v = cass().execute("SELECT release_version FROM system.local").one()[0]
    return f"keyspace retrasos, versión {v}"


def chk_neo():
    with neo().session() as s:
        return "GDS " + s.run("RETURN gds.version() AS v").single()["v"]


def chk_aws():
    import boto3
    ident = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
    return f"cuenta {ident}, credenciales válidas"


def chk_s3():
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    s3.head_bucket(Bucket=BUCKET)
    n = s3.list_objects_v2(Bucket=BUCKET, Prefix="vuelos_enriquecidos/").get("KeyCount", 0)
    return f"{BUCKET}: {n} objetos Parquet"


def chk_athena():
    r = athena("SELECT count(*) AS vuelos FROM reto7.vuelos")
    return f"tabla reto7.vuelos: {int(r['rows'][0][0]):,} vuelos"


def chk_spark():
    import urllib.request
    urllib.request.urlopen("http://localhost:8080", timeout=3)
    return "UI en :8080 (opcional)"


@app.get("/api/estado")
def estado():
    comp = [("Kafka", chk_kafka), ("Cassandra", chk_cass), ("Neo4j", chk_neo), ("AWS", chk_aws),
            ("S3", chk_s3), ("Athena", chk_athena), ("Spark", chk_spark)]

    def uno(c):
        try:
            return dict(n=c[0], ok=True, msg=c[1]())
        except Exception as e:
            return dict(n=c[0], ok=False, msg=f"{type(e).__name__}: {str(e)[:120]}")
    with ThreadPoolExecutor(8) as ex:
        return jsonify(list(ex.map(uno, comp)))


# ---------- Flujo Kafka -> Cassandra ----------
@app.get("/api/flujo")
def flujo():
    filas = int(request.args.get("filas", 5000))
    patron = request.args.get("patron", "data/bts/2026/*.csv")
    pasos = [("productor", [sys.executable, "-u", "-m", "ingesta.productor", "--archivos", patron, "--max-filas", str(filas)]),
             ("consumidor", [sys.executable, "-u", "-m", "ingesta.consumidor_cassandra", "--salir-al-terminar"])]
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    sse = lambda d: f"data: {json.dumps(d)}\n\n"

    def gen():
        for nombre, cmd in pasos:
            yield sse(dict(etapa=nombre, estado="inicio"))
            p = subprocess.Popen(cmd, cwd=RAIZ, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 text=True, encoding="utf-8", errors="replace")
            for linea in p.stdout:
                yield sse(dict(etapa=nombre, linea=linea.rstrip()))
            yield sse(dict(etapa=nombre, estado="fin", codigo=p.wait()))
        yield sse(dict(fin=True))
    return Response(gen(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def kafka_total():
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "demo-ui"})
    try:
        n = c.list_topics(TOPICO, timeout=5).topics[TOPICO].partitions
        return sum(h - l for l, h in (c.get_watermark_offsets(TopicPartition(TOPICO, p), timeout=5) for p in n))
    finally:
        c.close()


@app.get("/api/escala")
def escala():
    out = {}
    for k, f in {"kafka": lambda: kafka_total(),
                 "cassandra": lambda: {t: cass().execute(f"SELECT count(*) FROM {t}", timeout=120).one()[0] for t in
                                       ("eventos_por_aeropuerto_dia", "eventos_por_vuelo", "eventos_por_aeronave_mes")},
                 "neo4j": lambda: neo4j_conteos()}.items():
        t = time.time()
        try:
            out[k] = dict(v=f(), ms=round((time.time() - t) * 1000))
        except Exception as e:
            out[k] = dict(error=f"{type(e).__name__}: {str(e)[:100]}")
    return jsonify(out)


def neo4j_conteos():
    with neo().session() as s:
        return dict(aeropuertos=s.run("MATCH (a:Airport) RETURN count(a) AS n").single()["n"],
                    rutas=s.run("MATCH ()-[r:ROUTE]->() RETURN count(r) AS n").single()["n"])


# ---------- Consultas ----------
def tabla(cols, filas, ms, **extra):
    return jsonify(cols=list(cols), rows=[["" if v is None else str(v) for v in f] for f in filas[:200]], ms=ms, **extra)


@app.get("/api/cql/ejemplo")
def cql_ejemplo():
    s = cass()
    a = s.execute("SELECT airport, day FROM eventos_por_aeropuerto_dia LIMIT 1").one()
    return jsonify(airport=a.airport, dia=str(a.day))


@app.post("/api/cql")
def cql():
    d = request.json
    s, q, t = cass(), d["q"], time.time()
    if q == "aeropuerto":
        dia = date.fromisoformat(d["dia"])
        base = datetime(dia.year, dia.month, dia.day, tzinfo=timezone.utc)
        rs = s.execute("SELECT event_ts, event_type, carrier, flight_number, tail_number, delay_minutes "
                       "FROM eventos_por_aeropuerto_dia WHERE airport=%s AND day=%s AND event_ts>=%s AND event_ts<%s",
                       (d["airport"].upper(), dia, base + timedelta(hours=int(d["h1"])), base + timedelta(hours=int(d["h2"]))))
    elif q == "vuelo":
        rs = s.execute("SELECT event_ts, event_type, origin, dest, delay_minutes FROM eventos_por_vuelo "
                       "WHERE carrier=%s AND flight_number=%s LIMIT 20", (d["carrier"].upper(), int(d["numero"])))
    else:
        rs = s.execute("SELECT event_ts, event_type, origin, dest, delay_minutes FROM eventos_por_aeronave_mes "
                       "WHERE tail_number=%s AND month=%s", (d["tail"].upper(), d["month"]))
    filas = list(rs)
    return tabla(rs.column_names, filas, round((time.time() - t) * 1000))


CYPHER = {
    "pagerank": ("MATCH (a:Airport) RETURN a.iata AS aeropuerto, round(a.pagerank, 4) AS pagerank, "
                 "round(a.intermediacion, 1) AS intermediacion, a.salidas AS salidas, "
                 "a.retraso_salida_prom AS retraso_salida_prom ORDER BY a.pagerank DESC LIMIT 10"),
    "trafico": ("MATCH (a:Airport {iata: $cerrado})-[r:ROUTE]-() WITH sum(r.vuelos) AS afectado "
                "MATCH ()-[t:ROUTE]->() WITH afectado, sum(t.vuelos) AS total "
                "RETURN $cerrado AS cerrado, afectado AS vuelos_afectados, round(100.0 * afectado / total, 2) AS pct_trafico"),
    "aislados": ("MATCH (a:Airport) WHERE a.iata <> $cerrado AND NOT EXISTS "
                 "{ MATCH (a)-[:ROUTE]-(b:Airport) WHERE b.iata <> $cerrado } RETURN count(a) AS aeropuertos_aislados"),
}


@app.post("/api/cypher")
def cypher():
    d, t = request.json, time.time()
    p = {"cerrado": d.get("cerrado", "ATL").upper()}
    with neo().session() as s:
        if d["q"] == "pagerank":
            r = s.run(CYPHER["pagerank"])
            cols, filas = r.keys(), [list(x.values()) for x in r]
        else:
            a, b = s.run(CYPHER["trafico"], p).single(), s.run(CYPHER["aislados"], p).single()
            cols, filas = list(a.keys()) + list(b.keys()), [list(a.values()) + list(b.values())]
    return tabla(cols, filas, round((time.time() - t) * 1000))


SQL = {
    "total": "SELECT mes, count(*) AS vuelos FROM reto7.vuelos GROUP BY ROLLUP(mes) ORDER BY mes",
    "tramo": ("SELECT tramo_del_dia, count(*) AS vuelos, round(avg(dep_delay_new), 2) AS retraso_salida_prom_min, "
              "round(100.0 * sum(heredado) / nullif(sum(dep_delay_new), 0), 2) AS pct_heredado FROM reto7.vuelos "
              "WHERE tramo_del_dia <= 8 GROUP BY tramo_del_dia ORDER BY tramo_del_dia"),
    "franja": ("SELECT CASE WHEN hora_salida BETWEEN 0 AND 5 THEN '1 Madrugada (0-5)' WHEN hora_salida BETWEEN 6 AND 11 "
               "THEN '2 Manana (6-11)' WHEN hora_salida BETWEEN 12 AND 17 THEN '3 Tarde (12-17)' ELSE '4 Noche (18-23)' END AS franja, "
               "count(*) AS vuelos, round(avg(dep_delay_new), 2) AS retraso_salida_prom_min, "
               "round(avg(arr_delay_new), 2) AS retraso_llegada_prom_min FROM reto7.vuelos GROUP BY 1 ORDER BY 1"),
    "p90": ("SELECT op_unique_carrier AS aerolinea, count(*) AS vuelos, round(approx_percentile(arr_delay_new, 0.5), 1) AS mediana_llegada_min, "
            "round(approx_percentile(arr_delay_new, 0.9), 1) AS p90_llegada_min FROM reto7.vuelos "
            "GROUP BY op_unique_carrier ORDER BY p90_llegada_min DESC"),
}


def athena(sql):
    import boto3
    a = boto3.client("athena", region_name=REGION)
    qid = a.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": "reto7"},
                                  ResultConfiguration={"OutputLocation": SALIDA})["QueryExecutionId"]
    for _ in range(240):
        e = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if e["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if e["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(e["Status"].get("StateChangeReason", e["Status"]["State"]))
    filas = a.get_query_results(QueryExecutionId=qid, MaxResults=100)["ResultSet"]["Rows"]
    datos = [[c.get("VarCharValue", "") for c in f["Data"]] for f in filas]
    st = e["Statistics"]
    return dict(cols=datos[0], rows=datos[1:], ms=st.get("TotalExecutionTimeInMillis"),
                motor_ms=st.get("EngineExecutionTimeInMillis"), bytes=st.get("DataScannedInBytes", 0))


@app.post("/api/athena")
def athena_ep():
    q = request.json["q"]
    r = athena(SQL[q])
    if q == "total":
        for f in r["rows"]:
            f[0] = f[0] or "TOTAL"
    return jsonify(r)


if __name__ == "__main__":
    app.run(port=5000, threaded=True)
