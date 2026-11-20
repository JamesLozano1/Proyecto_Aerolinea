"""Compacta un Parquet particionado por `mes` en ~1 archivo por partición.

Spark deja muchos archivos pequeños cuando escribe tras una ventana (shuffle de 200 particiones).
Eso es ineficiente en S3 y en BigQuery. Este script lee el resultado ya calculado (sin recalcular la
cadena de vuelos) y lo reescribe compacto.

  spark-submit --master local[2] /opt/app/spark/compactar_parquet.py \
      --entrada /data/salida_8meses/vuelos_enriquecidos --salida /data/salida_8meses/vuelos_compacto
"""
import argparse

from pyspark.sql import SparkSession


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", required=True)
    ap.add_argument("--salida", required=True)
    args = ap.parse_args()
    spark = SparkSession.builder.appName("compactar-parquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(args.entrada)
    print(f"Filas: {df.count():,}")
    df.repartition("mes").write.mode("overwrite").partitionBy("mes").parquet(args.salida)
    spark.stop()


if __name__ == "__main__":
    main()
