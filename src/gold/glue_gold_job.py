"""
AWS Glue (PySpark) - camada GOLD.

Le os datasets da Silver (resultado_municipio, resultado_uf, metas_*) e
produz os 3 datasets analiticos pedidos no desafio:

  1. indicador_por_municipio  -> foto mais recente da taxa de
     alfabetizacao por municipio/UF.
  2. comparacao_metas_resultados -> taxa realizada vs. meta definida para
     aquele mesmo ano (a Silver ja despivotou as metas de wide para long,
     entao aqui e so um join por id_municipio + ano == ano_meta + rede).
  3. evolucao_temporal_indicador -> serie historica por UF e por Brasil
     (media da taxa realizada por ano).

Parametros: --JOB_NAME, --DATALAKE_BUCKET, --GLUE_DATABASE.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window, functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET", "GLUE_DATABASE"])
BUCKET = args["DATALAKE_BUCKET"]
DATABASE = args["GLUE_DATABASE"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def read_silver(entity: str) -> DataFrame:
    return spark.read.parquet(f"s3://{BUCKET}/silver/{entity}/")


def write_gold(df: DataFrame, entity: str, partition_cols: list[str]) -> None:
    path = f"s3://{BUCKET}/gold/{entity}/"
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
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=f"gold_{entity}")
    sink.writeFrame(dyf)


def main():
    resultado_municipio = read_silver("resultado_municipio")
    resultado_uf = read_silver("resultado_uf")
    metas_municipio = read_silver("metas_municipio")

    # 1) Indicador mais recente por municipio (uma linha por municipio+rede+serie).
    janela_recente = Window.partitionBy("id_municipio", "rede", "serie").orderBy(
        F.col("ano").desc()
    )
    indicador_por_municipio = (
        resultado_municipio
        .withColumn("rn", F.row_number().over(janela_recente))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "id_municipio", "nome_municipio", "sigla_uf", "ano", "rede", "serie",
            "taxa_alfabetizacao",
        )
    )
    write_gold(indicador_por_municipio, "indicador_por_municipio", partition_cols=["sigla_uf"])

    # 2) Comparacao meta vs. resultado: junta o resultado do ano X com a meta
    # que havia sido definida (em qualquer medicao anterior) PARA o ano X.
    comparacao = (
        resultado_municipio.alias("r")
        .join(
            metas_municipio.alias("m"),
            on=[
                F.col("r.id_municipio") == F.col("m.id_municipio"),
                F.col("r.rede") == F.col("m.rede"),
                F.col("r.ano") == F.col("m.ano_meta"),
            ],
            how="inner",
        )
        .select(
            F.col("r.id_municipio").alias("id_municipio"),
            F.col("r.sigla_uf").alias("sigla_uf"),
            F.col("r.ano").alias("ano"),
            F.col("r.rede").alias("rede"),
            F.col("r.taxa_alfabetizacao").alias("resultado_realizado"),
            F.col("m.meta_valor").alias("meta_definida"),
        )
        .withColumn("gap_percentual", F.col("resultado_realizado") - F.col("meta_definida"))
    )
    write_gold(comparacao, "comparacao_metas_resultados", partition_cols=["ano"])

    # 3) Evolucao temporal: media da taxa realizada por UF/ano e Brasil/ano.
    evolucao_uf = (
        resultado_uf.groupBy("sigla_uf", "ano")
        .agg(F.avg("taxa_alfabetizacao").alias("percentual_alfabetizado_medio"))
        .withColumn("nivel", F.lit("UF"))
    )
    evolucao_brasil = (
        resultado_uf.groupBy("ano")
        .agg(F.avg("taxa_alfabetizacao").alias("percentual_alfabetizado_medio"))
        .withColumn("sigla_uf", F.lit("BR"))
        .withColumn("nivel", F.lit("BRASIL"))
        .select("sigla_uf", "ano", "percentual_alfabetizado_medio", "nivel")
    )
    evolucao = evolucao_uf.unionByName(evolucao_brasil)
    write_gold(evolucao, "evolucao_temporal_indicador", partition_cols=["nivel"])

    job.commit()


if __name__ == "__main__":
    main()
