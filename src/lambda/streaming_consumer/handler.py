"""
Lambda acionada pelo Kinesis Data Stream (event source mapping).
Recebe lotes de eventos de "atualizacao de indicador", decodifica,
agrupa e grava como Parquet/JSON-lines na camada Bronze do S3, em um
prefixo separado da ingestao batch:

    s3://<bucket>/bronze/streaming_indicador/dt=YYYY-MM-DD/hh=HH/<uuid>.json

Mantido deliberadamente simples (sem pandas/pyarrow) para reduzir o
tamanho do pacote de deploy e o cold start da Lambda -- outra decisao
de FinOps (menos memoria/tempo de execucao = menos custo por invocacao).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)

BUCKET = os.environ["DATALAKE_BUCKET"]
PREFIX = os.environ.get("STREAMING_BRONZE_PREFIX", "bronze/streaming_indicador")

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")


def handler(event, context):
    records = event.get("Records", [])
    decoded = []
    errors = 0

    for record in records:
        try:
            payload = base64.b64decode(record["kinesis"]["data"])
            decoded.append(json.loads(payload.decode("utf-8").strip()))
        except Exception:
            errors += 1
            log.exception("Falha ao decodificar registro do Kinesis")

    if decoded:
        now = datetime.now(timezone.utc)
        key = (
            f"{PREFIX}/dt={now:%Y-%m-%d}/hh={now:%H}/"
            f"{uuid.uuid4()}.jsonl"
        )
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in decoded)
        s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))
        log.info("Gravado %s com %d eventos", key, len(decoded))

    # Metricas customizadas de observabilidade (FinOps + monitoramento):
    # volume processado e falhas de ingestao, visiveis no CloudWatch.
    cloudwatch.put_metric_data(
        Namespace="TechChallengeAlfabetizacao",
        MetricData=[
            {"MetricName": "StreamingEventsProcessed", "Value": len(decoded), "Unit": "Count"},
            {"MetricName": "StreamingDecodeErrors", "Value": errors, "Unit": "Count"},
        ],
    )

    return {"processed": len(decoded), "errors": errors}
