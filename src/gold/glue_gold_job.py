"""
AWS Glue (PySpark) - camada GOLD.

Le os datasets da Silver (resultado_municipio, resultado_uf, metas_*) e
produz os 3 datasets analiticos pedidos no desafio:

  1. indicador_por_municipio  -> foto mais recente da taxa de
     alfabetizacao por municipio (rede Municipal).
  2. comparacao_metas_resultados -> taxa realizada vs. meta definida para
     aquele mesmo ano (a Silver ja despivotou as metas de wide para long,
     entao aqui e so um join por id_municipio + ano == ano_meta).
  3. evolucao_temporal_indicador -> serie historica por UF e por Brasil
     (media da taxa realizada por ano, rede Publica).

Parametros: --JOB_NAME, --DATALAKE_BUCKET, --GLUE_DATABASE.
"""
import sys
import logging

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window, functions as F

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET", "GLUE_DATABASE"])
BUCKET = args["DATALAKE_BUCKET"]
DATABASE = args["GLUE_DATABASE"]

log.info("=" * 60)
log.info(f"JOB     : {args['JOB_NAME']}")
log.info(f"CAMADA  : GOLD -> s3://{BUCKET}/gold/")
log.info(f"CATALOGO: {DATABASE}")
log.info("=" * 60)

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

# Codigos INEP da coluna "rede" (ver tabela "dicionario" do dataset
# br_inep_avaliacao_alfabetizacao): 0=Total, 1=Federal, 2=Estadual,
# 3=Municipal, 4=Privada, 5=Publica (Estadual+Municipal), 6=Publica
# (Federal+Estadual+Municipal). As tabelas de meta usam texto fixo em vez
# de codigo -- meta_alfabetizacao_municipio e sempre "Municipal", e
# meta_alfabetizacao_uf/brasil sao sempre "Publica". Para manter todo o
# Gold na mesma rede/ano e comparavel com a meta correspondente, fixamos:
REDE_MUNICIPAL = 3   # bate com o escopo de meta_alfabetizacao_municipio
REDE_PUBLICA_UF = 5  # bate com o escopo de meta_alfabetizacao_uf/brasil

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def read_silver(entity: str) -> DataFrame:
    return spark.read.parquet(f"s3://{BUCKET}/silver/{entity}/")


def write_gold(df: DataFrame, entity: str, partition_cols: list[str]) -> int:
    path = f"s3://{BUCKET}/gold/{entity}/"
    log.info(f"[GOLD] Salvando '{entity}' em: {path}")

    # O sink do Glue NAO sobrescreve por padrao -- so acrescenta arquivos, e
    # o catalogo ACUMULA colunas entre execucoes. Como cada execucao deve
    # refletir um recalculo completo (nao um incremento), limpa os dois
    # antes de escrever.
    glueContext.purge_s3_path(path, options={"retentionPeriod": 0})
    drop_table_if_exists(f"gold_{entity}")
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
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=f"gold_{entity}")
    sink.writeFrame(dyf)

    count = df.count()
    log.info(f"[GOLD] '{entity}': {count} registros salvos | tabela=gold_{entity}")
    return count


def main():
    resultados = {}

    log.info("[GOLD] Lendo Silver: resultado_municipio, resultado_uf, metas_municipio")
    resultado_municipio = read_silver("resultado_municipio")
    resultado_uf = read_silver("resultado_uf")
    metas_municipio = read_silver("metas_municipio")

    # 1) Indicador mais recente por municipio (rede Municipal, unica linha
    # por municipio -- consistente com o escopo de meta_alfabetizacao_municipio).
    log.info("[GOLD] Gerando indicador_por_municipio")
    resultado_municipio_municipal = resultado_municipio.filter(F.col("rede") == REDE_MUNICIPAL)
    janela_recente = Window.partitionBy("id_municipio").orderBy(F.col("ano").desc())
    indicador_por_municipio = (
        resultado_municipio_municipal
        .withColumn("rn", F.row_number().over(janela_recente))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "id_municipio", "nome_municipio", "sigla_uf", "ano", "taxa_alfabetizacao",
        )
    )
    resultados["indicador_por_municipio"] = write_gold(
        indicador_por_municipio, "indicador_por_municipio", partition_cols=["sigla_uf"]
    )

    # 2) Comparacao meta vs. resultado: junta o resultado do ano X com a meta
    # que havia sido definida PARA o ano X (mesmo filtro de rede do item 1).
    log.info("[GOLD] Gerando comparacao_metas_resultados")
    comparacao = (
        resultado_municipio_municipal.alias("r")
        .join(
            metas_municipio.alias("m"),
            on=[
                F.col("r.id_municipio") == F.col("m.id_municipio"),
                F.col("r.ano") == F.col("m.ano_meta"),
            ],
            how="inner",
        )
        .select(
            F.col("r.id_municipio").alias("id_municipio"),
            F.col("r.sigla_uf").alias("sigla_uf"),
            F.col("r.ano").alias("ano"),
            F.col("r.taxa_alfabetizacao").alias("resultado_realizado"),
            F.col("m.meta_valor").alias("meta_definida"),
        )
        .withColumn("gap_percentual", F.col("resultado_realizado") - F.col("meta_definida"))
    )
    resultados["comparacao_metas_resultados"] = write_gold(
        comparacao, "comparacao_metas_resultados", partition_cols=["ano"]
    )

    # 3) Evolucao temporal: media da taxa realizada por UF/ano e Brasil/ano,
    # rede Publica (Estadual+Municipal) -- consistente com o escopo de
    # meta_alfabetizacao_uf/brasil.
    log.info("[GOLD] Gerando evolucao_temporal_indicador")
    resultado_uf_publica = resultado_uf.filter(F.col("rede") == REDE_PUBLICA_UF)
    evolucao_uf = (
        resultado_uf_publica.groupBy("sigla_uf", "ano")
        .agg(F.avg("taxa_alfabetizacao").alias("percentual_alfabetizado_medio"))
        .withColumn("nivel", F.lit("UF"))
    )
    evolucao_brasil = (
        resultado_uf_publica.groupBy("ano")
        .agg(F.avg("taxa_alfabetizacao").alias("percentual_alfabetizado_medio"))
        .withColumn("sigla_uf", F.lit("BR"))
        .withColumn("nivel", F.lit("BRASIL"))
        .select("sigla_uf", "ano", "percentual_alfabetizado_medio", "nivel")
    )
    evolucao = evolucao_uf.unionByName(evolucao_brasil)
    resultados["evolucao_temporal_indicador"] = write_gold(
        evolucao, "evolucao_temporal_indicador", partition_cols=["nivel"]
    )

    log.info("=" * 60)
    log.info("SUMARIO GOLD")
    for entidade, total in resultados.items():
        log.info(f"  {entidade:<28}: {total} registros")
    log.info(f"  Pipeline completo: bronze -> silver -> gold")
    log.info("=" * 60)

    job.commit()


if __name__ == "__main__":
    main()
