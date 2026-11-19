"""Fase 2 - ¿Qué proporción del retraso se hereda del vuelo anterior de la misma aeronave?

Método
------
1. Se ordenan los tramos de cada matrícula (TAIL_NUM) por hora programada de salida en UTC
   (hora local hhmm + zona horaria del aeropuerto de origen, de OpenFlights).
2. Con una ventana `lag` se obtiene el tramo anterior de la misma aeronave. El vínculo es válido si:
   el destino anterior == origen actual, el tramo anterior no se desvió y el tiempo programado en tierra
   está entre 0 y --gap-max-horas (de noche el retraso se absorbe).
3. Retraso heredado = min(retraso de salida, max(0, retraso de llegada anterior - holgura)),
   con holgura = max(0, tiempo programado en tierra - --turnaround-minimo).
   Es decir: del retraso con que llegó el avión, se descuenta la holgura que ya traía el itinerario.
4. Proporción heredada = minutos heredados / minutos totales de retraso de salida.
5. Contraste: BTS reporta LateAircraftDelay (solo en vuelos con llegada tardía >= 15 min), que se
   compara con la proporción propia sobre esos mismos vuelos.

Uso (dentro del contenedor, ver README):
  spark-submit --master local[2] /opt/app/spark/herencia_retraso.py \
      --entrada "/data/bts/2026/*.csv" --aeropuertos /data/openflights/airports.dat --salida /data/salida
"""
import argparse

from pyspark.sql import SparkSession, Window, functions as F

NUMERICAS = ["OP_CARRIER_FL_NUM", "CRS_DEP_TIME", "DEP_DELAY_NEW", "ARR_DELAY_NEW", "ARR_DEL15",
             "CANCELLED", "DIVERTED", "CRS_ELAPSED_TIME", "LATE_AIRCRAFT_DELAY"]
TEXTO = ["FL_DATE", "OP_UNIQUE_CARRIER", "TAIL_NUM", "ORIGIN", "DEST"]


def resumen(df, *claves):
    g = df.groupBy(*claves) if claves else df.groupBy()
    return g.agg(
        F.count("*").alias("vuelos"),
        F.round(F.avg("DEP_DELAY_NEW"), 2).alias("retraso_salida_prom_min"),
        F.sum("DEP_DELAY_NEW").alias("min_retraso_salida"),
        F.sum("heredado").alias("min_heredados"),
        F.round(100 * F.sum("heredado") / F.sum("DEP_DELAY_NEW"), 2).alias("pct_heredado"),
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--entrada", required=True)
    ap.add_argument("--aeropuertos", required=True)
    ap.add_argument("--salida", required=True)
    ap.add_argument("--turnaround-minimo", type=int, default=30, help="minutos mínimos en tierra")
    ap.add_argument("--gap-max-horas", type=int, default=6)
    ap.add_argument("--min-vuelos-aeropuerto", type=int, default=1000)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("herencia-retraso")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---- Lectura y tipos -------------------------------------------------------------
    crudo = spark.read.option("header", True).csv(args.entrada)
    df = crudo.select(*TEXTO, *[F.col(c).cast("double").alias(c) for c in NUMERICAS])
    df = df.withColumn("fecha", F.coalesce(F.to_date("FL_DATE", "yyyy-MM-dd"),
                                           F.to_date("FL_DATE", "M/d/yyyy h:mm:ss a")))

    # ---- Zona horaria del origen -> hora programada de salida/llegada en UTC -----------
    zonas = (spark.read.csv(args.aeropuertos)
             .select(F.col("_c4").alias("ORIGIN"), F.col("_c11").alias("tz"))
             .where((F.col("ORIGIN") != "\\N") & (F.col("tz") != "\\N") & F.col("tz").isNotNull()))
    df = df.join(F.broadcast(zonas), "ORIGIN", "inner")

    hh, mm = F.floor(F.col("CRS_DEP_TIME") / 100), F.col("CRS_DEP_TIME") % 100
    local = F.timestamp_seconds(F.unix_timestamp(F.col("fecha").cast("timestamp")) + hh * 3600 + mm * 60)
    df = df.withColumn("dep_s", F.unix_timestamp(F.to_utc_timestamp(local, F.col("tz"))))
    df = df.withColumn("arr_s", F.col("dep_s") + F.col("CRS_ELAPSED_TIME") * 60)

    # ---- Cadena de vuelos por aeronave -----------------------------------------------
    base = df.where((F.col("CANCELLED") == 0) & F.col("TAIL_NUM").isNotNull() & (F.col("TAIL_NUM") != "")
                    & F.col("dep_s").isNotNull())
    w = Window.partitionBy("TAIL_NUM").orderBy("dep_s", "OP_CARRIER_FL_NUM")
    base = (base.withColumn("prev_dest", F.lag("DEST").over(w))
            .withColumn("prev_arr_s", F.lag("arr_s").over(w))
            .withColumn("prev_arr_delay", F.lag("ARR_DELAY_NEW").over(w))
            .withColumn("prev_diverted", F.lag("DIVERTED").over(w)))

    gap_min = (F.col("dep_s") - F.col("prev_arr_s")) / 60
    vinculo = ((F.col("prev_dest") == F.col("ORIGIN")) & (F.col("prev_diverted") == 0)
               & F.col("prev_arr_delay").isNotNull()
               & (gap_min >= 0) & (gap_min <= args.gap_max_horas * 60))
    holgura = F.greatest(F.lit(0.0), gap_min - args.turnaround_minimo)
    heredado = F.least(F.col("DEP_DELAY_NEW"), F.greatest(F.lit(0.0), F.col("prev_arr_delay") - holgura))

    base = (base.withColumn("vinculo", vinculo)
            .withColumn("gap_tierra_min", gap_min)
            .withColumn("heredado", F.when(F.col("DEP_DELAY_NEW").isNull(), None)
                        .when(F.col("vinculo"), heredado).otherwise(F.lit(0.0)))
            .withColumn("tramo_del_dia", F.row_number().over(
                Window.partitionBy("TAIL_NUM", "fecha").orderBy("dep_s", "OP_CARRIER_FL_NUM")))
            .withColumn("hora_salida", (F.floor(F.col("CRS_DEP_TIME") / 100) % 24).cast("int"))
            .withColumn("mes", F.month("fecha")))

    enriquecido = base.select(
        "fecha", "mes", "OP_UNIQUE_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "hora_salida",
        "tramo_del_dia", "DEP_DELAY_NEW", "ARR_DELAY_NEW", "ARR_DEL15", "LATE_AIRCRAFT_DELAY",
        "vinculo", "gap_tierra_min", "heredado")
    ruta_parquet = f"{args.salida}/vuelos_enriquecidos"
    enriquecido.write.mode("overwrite").partitionBy("mes").parquet(ruta_parquet)

    # ---- Agregaciones (se lee lo escrito para no recalcular la cadena) ----------------
    v = spark.read.parquet(ruta_parquet)

    print("\n=== GLOBAL ===")
    glob = resumen(v)
    glob.show(truncate=False)
    print("Vuelos con vínculo válido con el tramo anterior:")
    v.agg(F.count("*").alias("vuelos"), F.round(100 * F.avg(F.col("vinculo").cast("int")), 2).alias("pct_con_vinculo")).show()

    print("=== CONTRASTE CON BTS (solo vuelos con llegada tardía >= 15 min) ===")
    comp = v.where(F.col("ARR_DEL15") == 1).agg(
        F.count("*").alias("vuelos_tardios"),
        F.round(100 * F.sum("LATE_AIRCRAFT_DELAY") / F.sum("ARR_DELAY_NEW"), 2).alias("pct_late_aircraft_BTS_sobre_llegada"),
        F.round(100 * F.sum("heredado") / F.sum("DEP_DELAY_NEW"), 2).alias("pct_heredado_propio_sobre_salida"))
    comp.show(truncate=False)

    salidas = {
        "global": glob,
        "contraste_bts": comp,
        "por_aerolinea": resumen(v, "OP_UNIQUE_CARRIER").orderBy(F.desc("vuelos")),
        "por_hora": resumen(v, "hora_salida").orderBy("hora_salida"),
        "por_mes": resumen(v, "mes").orderBy("mes"),
        "por_tramo_del_dia": resumen(v.where(F.col("tramo_del_dia") <= 10), "tramo_del_dia").orderBy("tramo_del_dia"),
        "por_aeropuerto_origen": resumen(v, "ORIGIN").where(F.col("vuelos") >= args.min_vuelos_aeropuerto)
                                 .orderBy(F.desc("pct_heredado")),
    }
    for nombre, tabla in salidas.items():
        if nombre not in ("global", "contraste_bts"):
            print(f"=== {nombre.upper()} ===")
            tabla.show(15, truncate=False)
        tabla.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.salida}/resumenes/{nombre}")

    spark.stop()


if __name__ == "__main__":
    main()
