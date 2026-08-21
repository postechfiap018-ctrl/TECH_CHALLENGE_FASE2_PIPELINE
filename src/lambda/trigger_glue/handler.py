"""
Lambda acionada por evento S3 ObjectCreated no prefixo bronze/.
Dispara o job Glue de Silver, orientando a pipeline por evento em vez de
por agendamento fixo (reduz custo: o job so roda quando ha dado novo).

Ha um debounce simples: se o job Silver ja estiver RUNNING/STARTING,
a Lambda nao dispara uma segunda execucao (evita jobs concorrentes e
gasto duplicado de DPU-hora).
"""
from __future__ import annotations

import logging
import os

import boto3

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)

GLUE_SILVER_JOB_NAME = os.environ["GLUE_SILVER_JOB_NAME"]

glue = boto3.client("glue")


def _job_already_running(job_name: str) -> bool:
    runs = glue.get_job_runs(JobName=job_name, MaxResults=5)["JobRuns"]
    return any(r["JobRunState"] in ("STARTING", "RUNNING") for r in runs)


def handler(event, context):
    triggered_keys = [
        rec["s3"]["object"]["key"]
        for rec in event.get("Records", [])
        if "s3" in rec
    ]
    log.info("Evento S3 recebido para: %s", triggered_keys)

    if _job_already_running(GLUE_SILVER_JOB_NAME):
        log.info("Job %s ja em execucao, ignorando trigger.", GLUE_SILVER_JOB_NAME)
        return {"started": False, "reason": "already_running"}

    response = glue.start_job_run(JobName=GLUE_SILVER_JOB_NAME)
    log.info("Job %s iniciado: %s", GLUE_SILVER_JOB_NAME, response["JobRunId"])
    return {"started": True, "job_run_id": response["JobRunId"]}
