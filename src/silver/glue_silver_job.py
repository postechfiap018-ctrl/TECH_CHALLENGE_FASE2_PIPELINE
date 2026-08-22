"""
AWS Glue (PySpark) - camada SILVER.

Le as entidades da camada Bronze (batch + streaming), aplica limpeza,
padronizacao de nomes/tipos, normalizacao de chaves e integra:

  - Dimensao territorial: municipio + uf (nome, sigla_uf), do dataset
    br_bd_diretorios_brasil.
  - Resultado realizado do indicador (taxa_alfabetizacao) por municipio e
    por UF, do dataset br_inep_avaliacao_alfabetizacao.
  - Metas de alfabetizacao (Brasil/UF/Municipio): as tabelas de origem tem
    uma coluna por ano-alvo (meta_alfabetizacao_2024 .. meta_alfabetizacao_2030,
    "wide"). Aqui elas sao despivotadas para o formato longo
    (ano_meta, meta_valor), o que permite comparar cada ano realizado com a
    meta definida para aquele mesmo ano na camada Gold.
  - Alunos: microdados da avaliacao (uma linha por aluno/ano).

Grava cada dataset limpo em Parquet na camada Silver e cataloga
automaticamente no Glue Data Catalog (via updateBehavior=UPDATE_IN_DATABASE),
sem depender de um crawler separado.

Parametros (injetados pelo Glue): --JOB_NAME, --DATALAKE_BUCKET, --GLUE_DATABASE.
"""
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET", "GLUE_DATABASE"])
BUCKET = args["DATALAKE_BUCKET"]
DATABASE = args["GLUE_DATABASE"]

glue_client = boto3.client("glue")


def drop_table_if_exists(table_name: str) -> None:
    """updateBehavior=UPDATE_IN_DATABASE faz o catalogo ACUMULAR colunas
    entre execucoes em vez de substituir o schema. Apagar a tabela antes de
    escrever garante que o catalogo sempre reflita o schema atual."""
    try:
        glue_client.delete_table(DatabaseName=DATABASE, Name=table_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise

META_YEAR_COLUMNS = [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]

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


def unpivot_metas(df: DataFrame, group_cols: list[str]) -> DataFrame:
    """Transforma as colunas meta_alfabetizacao_2024..2030 (wide) em duas
    colunas (ano_meta, meta_valor), no formato longo. Mantem as demais
    colunas de agrupamento (ex.: id_municipio, sigla_uf, rede) e renomeia
    o "ano" original (ano da medicao-base) para "ano_base" para nao colidir
    com o novo "ano_meta"."""
    existing_year_cols = [c for c in META_YEAR_COLUMNS if c in df.columns]
    stack_expr = ", ".join(f"{c.split('_')[-1]}, `{c}`" for c in existing_year_cols)
    n = len(existing_year_cols)

    if "ano" in df.columns:
        df = df.withColumnRenamed("ano", "ano_base")

    select_cols = [c for c in group_cols if c in df.columns]
    if "ano_base" in df.columns:
        select_cols = select_cols + ["ano_base"]

    unpivoted = df.select(
        *select_cols,
        F.expr(f"stack({n}, {stack_expr}) as (ano_meta, meta_valor)"),
    ).withColumn("ano_meta", F.col("ano_meta").cast("int"))

    # A meta para um dado ano_meta e republicada em toda medicao-base
    # subsequente (ex.: a meta 2024 aparece tanto na medicao de 2023 quanto
    # na de 2024) -- sem isso o join da Gold gera linha duplicada. Mantem
    # so a versao mais recente (maior ano_base) de cada meta.
    if "ano_base" in unpivoted.columns:
        dedup_keys = [c for c in group_cols if c in unpivoted.columns] + ["ano_meta"]
        janela = Window.partitionBy(*dedup_keys).orderBy(F.col("ano_base").desc())
        unpivoted = (
            unpivoted.withColumn("rn", F.row_number().over(janela))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )
    return unpivoted


def write_silver(df: DataFrame, entity: str, partition_cols: list[str]) -> None:
    # Cataloga automaticamente no Glue Data Catalog (visivel no Athena) via
    # updateBehavior=UPDATE_IN_DATABASE, sem depender de um crawler separado.
    path = f"s3://{BUCKET}/silver/{entity}/"
    # O sink do Glue NAO sobrescreve por padrao -- so acrescenta arquivos, e
    # o catalogo ACUMULA colunas entre execucoes. Como cada execucao deve
    # refletir um recalculo completo (nao um incremento), limpa os dois
    # antes de escrever.
    glueContext.purge_s3_path(path, options={"retentionPeriod": 0})
    drop_table_if_exists(f"silver_{entity}")
    dyf = DynamicFrame.fromDF(df, glueContext, entity)
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
    # --- Dimensao territorial -------------------------------------------------
    # br_bd_diretorios_brasil.uf usa a coluna "sigla" (nao "sigla_uf" como as
    # demais fontes) -- renomeia logo na entrada para integrar com o resto.
    raw_uf = read_bronze("uf")
    if "sigla" in raw_uf.columns:
        raw_uf = raw_uf.withColumnRenamed("sigla", "sigla_uf")
    uf_dim = clean(raw_uf, ["sigla_uf"]).select(
        "sigla_uf", F.col("nome").alias("nome_uf")
    )
    municipio_dim = clean(read_bronze("municipio"), ["id_municipio"]).select(
        "id_municipio", F.col("nome").alias("nome_municipio"), "sigla_uf"
    )
    write_silver(uf_dim, "uf", partition_cols=[])
    write_silver(municipio_dim, "municipio", partition_cols=[])

    # --- Resultado realizado (taxa_alfabetizacao) ------------------------------
    resultado_municipio = clean(
        read_bronze("municipio_resultado_alfabetizacao"),
        ["id_municipio", "ano", "rede", "serie"],
    )
    resultado_municipio_integrado = resultado_municipio.join(
        municipio_dim, on="id_municipio", how="left"
    )
    write_silver(
        resultado_municipio_integrado, "resultado_municipio", partition_cols=["ano"]
    )

    resultado_uf = clean(
        read_bronze("uf_resultado_alfabetizacao"), ["sigla_uf", "ano", "rede", "serie"]
    )
    resultado_uf_integrado = resultado_uf.join(uf_dim, on="sigla_uf", how="left")
    write_silver(resultado_uf_integrado, "resultado_uf", partition_cols=["ano"])

    # --- Metas (wide -> long: uma linha por ano-alvo) --------------------------
    meta_brasil = clean(read_bronze("meta_alfabetizacao_brasil"), ["ano", "rede"])
    metas_brasil_long = unpivot_metas(meta_brasil, group_cols=["rede"])
    write_silver(metas_brasil_long, "metas_brasil", partition_cols=[])

    meta_uf = clean(read_bronze("meta_alfabetizacao_uf"), ["sigla_uf", "ano", "rede"])
    metas_uf_long = unpivot_metas(meta_uf, group_cols=["sigla_uf", "rede"])
    write_silver(metas_uf_long, "metas_uf", partition_cols=[])

    meta_municipio = clean(
        read_bronze("meta_alfabetizacao_municipio"), ["id_municipio", "ano", "rede"]
    )
    metas_municipio_long = unpivot_metas(meta_municipio, group_cols=["id_municipio", "rede"])
    write_silver(metas_municipio_long, "metas_municipio", partition_cols=[])

    # --- Alunos (microdados) ---------------------------------------------------
    alunos = clean(read_bronze("alunos"), ["id_aluno", "ano"])
    write_silver(alunos, "alunos", partition_cols=["ano"])

    job.commit()


if __name__ == "__main__":
    main()
