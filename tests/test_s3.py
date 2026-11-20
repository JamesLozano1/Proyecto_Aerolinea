import boto3
import pytest

moto = pytest.importorskip("moto")

from analitica.subir_s3 import archivos_a_subir, asegurar_bucket, listar, subir  # noqa: E402


def test_sube_parquet_conservando_particiones(tmp_path, monkeypatch):
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(k, "test")
    for mes in (1, 2):
        d = tmp_path / f"mes={mes}"
        d.mkdir()
        (d / "part-0.parquet").write_bytes(b"x" * 100)
        (d / ".part-0.parquet.crc").write_bytes(b"c")   # oculto: no debe subir
    (tmp_path / "_SUCCESS").write_bytes(b"")             # no debe subir
    assert len(archivos_a_subir(tmp_path)) == 2

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        asegurar_bucket(s3, "mi-bucket", "us-east-1", crear=True)
        subir(s3, "mi-bucket", tmp_path, "vuelos_enriquecidos")
        claves = {o["Key"] for o in s3.list_objects_v2(Bucket="mi-bucket")["Contents"]}
        assert claves == {"vuelos_enriquecidos/mes=1/part-0.parquet", "vuelos_enriquecidos/mes=2/part-0.parquet"}
        assert listar(s3, "mi-bucket", "vuelos_enriquecidos")[0] == 2
