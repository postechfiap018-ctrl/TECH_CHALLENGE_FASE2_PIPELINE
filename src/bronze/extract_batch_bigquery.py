"""
Ingestao BATCH: extrai as entidades da Base dos Dados (BigQuery publico)
e grava em Parquet na camada Bronze do S3, particionado por data de
ingestao e entidade.

Pode ser rodado:
  - Localmente:      python -m src.bronze.extract_batch_bigquery
  - No notebook/Colab: chamado celula a celula (ver notebooks/pipeline_alfabetizacao.ipynb)

Pre-requisitos (ver README para o passo a passo completo):
  - Variavel de ambiente AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (ou perfil
    configurado via `aws configure`) com permissao de escrita no bucket.
  - GOOGLE_APPLICATION_CREDENTIALS apontando para o JSON da service account
    do GCP com permissao de leitura no BigQuery (roles/bigquery.jobUser +
    roles/bigquery.dataViewer no dataset publico basta o job user, pois o
    dataset "basedosdados" ja e publico).
"""
from __future__ import annotations

import io
import json
import logging
from datetime import date

import boto3
import pandas as pd
from google.cloud import bigquery

from src.config import (
    AWS_REGION,
    BRONZE_PREFIX,
    DATALAKE_BUCKET,
    GCP_BILLING_PROJECT,
    KEY_COLUMNS,
    QUERY_OVERRIDES,
    SOURCE_TABLES,
)
from src.quality.data_quality_checks import (
    check_referential_integrity,
    run_quality_report,
    save_report_to_s3,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_BILLING_PROJECT)


def _s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def extract_entity(entity: str, table_id: str, ingestion_date: date) -> pd.DataFrame:
    """Roda `SELECT * FROM table_id` no BigQuery e retorna um DataFrame.
    Para tabelas muito grandes (ex.: microdados de alunos), troque por uma
    query com filtro de ano/particao para nao escanear a tabela inteira
    (isso e uma pratica de FinOps: menos bytes escaneados = menos custo)."""
    if table_id.startswith("TODO_"):
        raise ValueError(
            f"Tabela de origem para '{entity}' ainda nao configurada em src/config.py "
            "(SOURCE_TABLES). Consulte basedosdados.org e preencha o table_id."
        )
    query = QUERY_OVERRIDES.get(entity, "SELECT * FROM `{table}`").format(table=table_id)
    log.info("Extraindo %s de %s", entity, table_id)
    client = _bq_client()
    df = client.query(query).to_dataframe()
    log.info("%s: %d linhas, %d colunas", entity, len(df), len(df.columns))
    return df


def upload_parquet_to_bronze(df: pd.DataFrame, entity: str, ingestion_date: date) -> str:
    """Grava o parquet particionado por entidade e data de ingestao:
    s3://bucket/bronze/<entidade>/dt_ingestao=YYYY-MM-DD/<entidade>.parquet
    """
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    key = (
        f"{BRONZE_PREFIX}/{entity}/dt_ingestao={ingestion_date.isoformat()}/"
        f"{entity}.parquet"
    )
    _s3_client().upload_fileobj(buf, DATALAKE_BUCKET, key)
    s3_uri = f"s3://{DATALAKE_BUCKET}/{key}"
    log.info("OK: %s", s3_uri)
    return s3_uri


# Pares (tabela filha, coluna) -> (tabela pai, coluna) para a checagem de
# consistencia entre tabelas exigida no desafio: toda chave estrangeira
# extraida precisa existir na tabela dimensao correspondente.
REFERENTIAL_CHECKS = [
    ("municipio_resultado_alfabetizacao", "id_municipio", "municipio", "id_municipio"),
    ("meta_alfabetizacao_municipio", "id_municipio", "municipio", "id_municipio"),
    ("alunos", "id_municipio", "municipio", "id_municipio"),
]


def run_batch_ingestion(ingestion_date: date | None = None) -> dict[str, str]:
    ingestion_date = ingestion_date or date.today()
    results: dict[str, str] = {}
    dataframes: dict[str, pd.DataFrame] = {}

    for entity, table_id in SOURCE_TABLES.items():
        try:
            df = extract_entity(entity, table_id, ingestion_date)
        except ValueError as exc:
            log.warning("Pulando '%s': %s", entity, exc)
            continue

        report = run_quality_report(
            df, key_columns=KEY_COLUMNS.get(entity, []), entity=entity
        )
        if not report.passed:
            log.warning("Qualidade com ressalvas em '%s': %s", entity, report.issues)

        save_report_to_s3(
            report,
            bucket=DATALAKE_BUCKET,
            key=(
                f"governance/quality-reports/bronze/{entity}/"
                f"dt_ingestao={ingestion_date.isoformat()}/report.json"
            ),
        )

        dataframes[entity] = df
        s3_uri = upload_parquet_to_bronze(df, entity, ingestion_date)
        results[entity] = s3_uri

    # Consistencia entre tabelas: confere se toda chave estrangeira extraida
    # existe na tabela dimensao correspondente.
    for child_entity, child_key, parent_entity, parent_key in REFERENTIAL_CHECKS:
        if child_entity not in dataframes or parent_entity not in dataframes:
            continue
        integrity = check_referential_integrity(
            dataframes[child_entity], dataframes[parent_entity], child_key, parent_key
        )
        if not integrity["passed"]:
            log.warning(
                "Integridade referencial: %d '%s' orfaos em '%s' (sem %s correspondente em '%s')",
                integrity["orphan_count"], child_key, child_entity, parent_key, parent_entity,
            )
        s3_client = _s3_client()
        s3_client.put_object(
            Bucket=DATALAKE_BUCKET,
            Key=(
                f"governance/quality-reports/referential-integrity/"
                f"{child_entity}_x_{parent_entity}/dt_ingestao={ingestion_date.isoformat()}/report.json"
            ),
            Body=json.dumps(integrity, ensure_ascii=False, default=str).encode("utf-8"),
            ContentType="application/json",
        )

    return results


if __name__ == "__main__":
    run_batch_ingestion()
