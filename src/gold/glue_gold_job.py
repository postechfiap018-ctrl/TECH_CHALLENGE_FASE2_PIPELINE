"""
AWS Glue (PySpark) - camada GOLD.

Le o dataset integrado da Silver (alfabetizacao_integrado) e produz 3
datasets analiticos, prontos para dashboards / Athena / treinamento de
modelos de ML:

  1. indicador_por_municipio  -> % de alfabetizacao mais recente por
     municipio/UF, pronto para mapa/ranking.
  2. comparacao_metas_resultados -> indicador realizado vs meta definida
     (nacional, estadual e municipal), com a diferenca (gap) calculada.
  3. evolucao_temporal_indicador -> serie historica do indicador por UF
     e Brasil, para grafico de evolucao ano a ano.

Parametros: --JOB_NAME, --DATALAKE_BUCKET, --GLUE_DATABASE (mesmo padrao
do job Silver).
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
    integrado = read_silver("alfabetizacao_integrado")

    # 1) Indicador mais recente por municipio.
    janela_recente = Window.partitionBy("id_municipio").orderBy(F.col("ano").desc())
    select_cols = ["id_municipio"]
    if "nome_municipio" in integrado.columns:
        select_cols.append("nome_municipio")
    select_cols += ["sigla_uf", "ano", "percentual_alfabetizado"]
    indicador_por_municipio = (
        integrado
        .withColumn("rn", F.row_number().over(janela_recente))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(*select_cols)
    )
    write_gold(indicador_por_municipio, "indicador_por_municipio", partition_cols=["sigla_uf"])

    # 2) Comparacao meta vs resultado (gap), quando a coluna de meta existir.
    meta_col_candidates = [c for c in integrado.columns if "meta" in c.lower()]
    if meta_col_candidates:
        meta_col = meta_col_candidates[0]
        comparacao = integrado.select(
            "id_municipio", "sigla_uf", "ano",
            F.col("percentual_alfabetizado").alias("resultado_realizado"),
            F.col(meta_col).alias("meta_definida"),
        ).withColumn(
            "gap_percentual", F.col("resultado_realizado") - F.col("meta_definida")
        )
        write_gold(comparacao, "comparacao_metas_resultados", partition_cols=["ano"])

    # 3) Evolucao temporal do indicador, agregado por UF e por Brasil.
    evolucao_uf = (
        integrado.groupBy("sigla_uf", "ano")
        .agg(F.avg("percentual_alfabetizado").alias("percentual_alfabetizado_medio"))
        .withColumn("nivel", F.lit("UF"))
    )
    evolucao_brasil = (
        integrado.groupBy("ano")
        .agg(F.avg("percentual_alfabetizado").alias("percentual_alfabetizado_medio"))
        .withColumn("sigla_uf", F.lit("BR"))
        .withColumn("nivel", F.lit("BRASIL"))
        .select("sigla_uf", "ano", "percentual_alfabetizado_medio", "nivel")
    )
    evolucao = evolucao_uf.unionByName(evolucao_brasil)
    write_gold(evolucao, "evolucao_temporal_indicador", partition_cols=["nivel"])

    job.commit()


if __name__ == "__main__":
    main()
