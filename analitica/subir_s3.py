"""Sube a S3 el histórico (Parquet u otros archivos) conservando la estructura de carpetas.

Las credenciales NO van en el código: boto3 las toma de las variables de entorno
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY y AWS_SESSION_TOKEN (las cuentas educativas con
credenciales temporales exigen el token) o de ~/.aws/credentials.

Ejemplos:
  python -m analitica.subir_s3 --bucket MI-BUCKET --origen data/salida_8meses/vuelos_compacto --prefijo vuelos_enriquecidos --crear-bucket
  python -m analitica.subir_s3 --bucket MI-BUCKET --prefijo vuelos_enriquecidos --listar
"""
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

EXTENSIONES = {".parquet", ".csv", ".json"}


def asegurar_bucket(s3, bucket, region, crear):
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        codigo = str(e.response["Error"]["Code"])
        if codigo in ("403", "AccessDenied"):
            sys.exit(f"El bucket '{bucket}' existe pero no es tuyo (o no tienes permiso). "
                     "Los nombres son únicos en todo AWS: elige otro.")
        if codigo not in ("404", "NoSuchBucket", "NotFound"):
            raise
    if not crear:
        sys.exit(f"El bucket '{bucket}' no existe. Usa --crear-bucket para crearlo.")
    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)
    print(f"Bucket creado: {bucket} ({region})")


def archivos_a_subir(origen: Path):
    return sorted(p for p in origen.rglob("*") if p.is_file() and p.suffix in EXTENSIONES
                  and not p.name.startswith((".", "_")))


def subir(s3, bucket, origen: Path, prefijo, hilos=8):
    archivos = archivos_a_subir(origen)
    if not archivos:
        sys.exit(f"No hay archivos .parquet/.csv/.json en {origen}")
    total = sum(p.stat().st_size for p in archivos)
    print(f"Subiendo {len(archivos)} archivos ({total / 1e6:.1f} MB) a s3://{bucket}/{prefijo}/ ...")

    def una(p):
        clave = f"{prefijo}/{p.relative_to(origen).as_posix()}"
        s3.upload_file(str(p), bucket, clave)
        return clave

    with ThreadPoolExecutor(max_workers=hilos) as ex:
        for i, _ in enumerate(ex.map(una, archivos), 1):
            if i % 50 == 0 or i == len(archivos):
                print(f"  {i}/{len(archivos)}")
    return len(archivos), total


def listar(s3, bucket, prefijo):
    n = tam = 0
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefijo):
        for o in pagina.get("Contents", []):
            n += 1
            tam += o["Size"]
            if n <= 15:
                print(f"  {o['Key']}  ({o['Size'] / 1e6:.2f} MB)")
    print(f"Total en s3://{bucket}/{prefijo}: {n} objetos, {tam / 1e6:.1f} MB")
    return n, tam


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--origen", help="carpeta local a subir")
    ap.add_argument("--prefijo", default="vuelos_enriquecidos", help="carpeta destino dentro del bucket")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--crear-bucket", action="store_true")
    ap.add_argument("--listar", action="store_true", help="solo lista lo que hay en S3")
    args = ap.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    try:
        asegurar_bucket(s3, args.bucket, args.region, args.crear_bucket)
        if args.listar:
            listar(s3, args.bucket, args.prefijo)
            return
        if not args.origen:
            sys.exit("Falta --origen (o usa --listar).")
        subir(s3, args.bucket, Path(args.origen), args.prefijo)
        print("\nVerificación:")
        listar(s3, args.bucket, args.prefijo)
    except NoCredentialsError:
        sys.exit("No hay credenciales de AWS. Define AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY y "
                 "AWS_SESSION_TOKEN (ver README).")
    except ClientError as e:
        if "ExpiredToken" in str(e) or "InvalidToken" in str(e):
            sys.exit("Las credenciales expiraron. Inicia el laboratorio y copia unas nuevas.")
        raise


if __name__ == "__main__":
    main()
