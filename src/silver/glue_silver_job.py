"""
AWS Glue (PySpark) - camada SILVER.

Le todas as entidades da camada Bronze (batch + streaming), aplica:
  - limpeza (drop de duplicatas exatas)
  - tratamento de nulos (chaves nulas sao descartadas com log; demais
    colunas numericas nulas viram 0 apenas quando fizer sentido de negocio
    -- aqui deixamos como null explicito e documentamos a decisao no README)
  - padronizacao de nomes/tipos (snake_case, sigla_uf upper, ids como string)
  - normalizacao de chaves (id_municipio com 7 digitos, zero-padded)
  - integracao: join de municipio + uf + metas + indicador em um dataset
    unico "alfabetizacao_integrado"

Grava o resultado particionado por ano em Parquet na camada Silver e
atualiza o Glue Data Catalog (para consulta via Athena).

Parametros esperados (--JOB_NAME e os defaults sao injetados pelo Glue):
    --DATALAKE_BUCKET   nome do bucket do data lake
    --GLUE_DATABASE     database do Glue Catalog

Deploy: este arquivo e enviado para
    s3://<bucket>/glue-scripts/glue_silver_job.py
pelo infra/provision_aws.py, que tambem cria o Glue Job apontando pra ele.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET", "GLUE_DATABASE"])
BUCKET = args["DATALAKE_BUCKET"]
DATABASE = args["GLUE_DATABASE"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def read_bronze(entity: str) -> DataFrame:
    path = f"s3://{BUCKET}/bronze/{entity}/"
    return spark.read.option("mergeSchema", "true").parquet(path)


def standardize_columns(df: DataFrame) -> DataFrame:
    for old in df.columns:
        new = old.strip().lower().replace(" ", "_")
        if new != old:
            df = df.withColumnRenamed(old, new)
    return df


def normalize_municipio_key(df: DataFrame, col: str = "id_municipio") -> DataFrame:
    if col in df.columns:
        df = df.withColumn(col, F.lpad(F.col(col).cast(StringType()), 7, "0"))
    return df


def normalize_uf_key(df: DataFrame, col: str = "sigla_uf") -> DataFrame:
    if col in df.columns:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))
    return df


def clean(df: DataFrame, key_columns: list[str]) -> DataFrame:
    df = standardize_columns(df)
    df = df.dropDuplicates()
    existing_keys = [c for c in key_columns if c in df.columns]
    if existing_keys:
        df = df.dropna(subset=existing_keys)
        df = df.dropDuplicates(subset=existing_keys)
    df = normalize_municipio_key(df)
    df = normalize_uf_key(df)
    return df


def write_silver(df: DataFrame, entity: str, partition_cols: list[str]) -> None:
    # Cataloga automaticamente no Glue Data Catalog (visivel no Athena)
    # via updateBehavior=UPDATE_IN_DATABASE, sem depender de um crawler
    # separado -- menos um recurso rodando (custo) e o catalogo fica
    # sempre consistente com o que o job acabou de escrever.
    path = f"s3://{BUCKET}/silver/{entity}/"
    dyf = glueContext.create_dynamic_frame.from_frame(df, name=entity)
    sink = glueContext.getSink(
        path=path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_cols,
        enableUpdateCatalog=True,
        transformation_ctx=f"sink_{entity}",
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=f"silver_{entity}")
    sink.writeFrame(dyf)


def main():
    uf = clean(read_bronze("uf"), ["sigla_uf"])
    municipio = clean(read_bronze("municipio"), ["id_municipio"])

    try:
        meta_brasil = clean(read_bronze("meta_alfabetizacao_brasil"), ["ano"])
    except Exception:
        meta_brasil = None
    try:
        meta_uf = clean(read_bronze("meta_alfabetizacao_uf"), ["sigla_uf", "ano"])
    except Exception:
        meta_uf = None
    try:
        meta_municipio = clean(read_bronze("meta_alfabetizacao_municipio"), ["id_municipio", "ano"])
    except Exception:
        meta_municipio = None
    try:
        indicador = clean(read_bronze("dados_alunos_indicador"), ["id_municipio", "ano"])
    except Exception:
        indicador = None

    write_silver(uf, "uf", partition_cols=[])
    write_silver(municipio, "municipio", partition_cols=[])

    # Integracao: municipio + uf (via sigla_uf) + indicador + metas.
    integrado = municipio.join(uf, on="sigla_uf", how="left")

    if indicador is not None:
        integrado = integrado.join(indicador, on="id_municipio", how="inner")
        if meta_municipio is not None:
            integrado = integrado.join(
                meta_municipio, on=["id_municipio", "ano"], how="left"
            )
        if meta_uf is not None:
            integrado = integrado.join(meta_uf, on=["sigla_uf", "ano"], how="left")
        if meta_brasil is not None:
            integrado = integrado.join(meta_brasil, on="ano", how="left")

        write_silver(integrado, "alfabetizacao_integrado", partition_cols=["ano"])
    else:
        # Sem o indicador ainda configurado em SOURCE_TABLES (ver src/config.py),
        # gravamos ao menos a base territorial integrada para nao bloquear o
        # restante da pipeline.
        write_silver(integrado, "territorio_integrado", partition_cols=[])

    job.commit()


if __name__ == "__main__":
    main()
