"""
Ingestao STREAMING (simulada): produz eventos de "atualizacao de
indicador" quase em tempo real e envia para um Kinesis Data Stream.

Por que simular? A fonte oficial (Base dos Dados / INEP) publica o
indicador em lotes (anual/periodico), sem um endpoint de eventos real.
O desafio pede para SIMULAR ingestao de eventos quase em tempo real,
entao este producer gera eventos sinteticos plausiveis (ex.: "resultado
parcial" de um municipio chegando) a partir dos dados batch ja carregados,
e os envia via put_record para o Kinesis. Isso demonstra o padrao de
ingestao streaming sem inventar uma API externa que nao existe.

Uso local (roda por --duration segundos, 1 evento a cada --interval s):
    python -m src.bronze.streaming_producer --duration 60 --interval 5

Em producao, isso seria disparado por um agendador (EventBridge Scheduler
ou um container leve), nao um loop infinito.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

from src.config import AWS_REGION, KINESIS_STREAM_NAME

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Amostra pequena de municipios/UF para gerar eventos sinteticos plausiveis.
# Em execucao real, isso pode ser lido do bronze/municipio mais recente.
_SAMPLE_MUNICIPIOS = [
    ("3550308", "SP", "Sao Paulo"),
    ("3304557", "RJ", "Rio de Janeiro"),
    ("2927408", "BA", "Salvador"),
    ("2304400", "CE", "Fortaleza"),
    ("1302603", "AM", "Manaus"),
    ("4106902", "PR", "Curitiba"),
]


def _kinesis_client():
    return boto3.client("kinesis", region_name=AWS_REGION)


def build_event() -> dict:
    id_municipio, sigla_uf, nome = random.choice(_SAMPLE_MUNICIPIOS)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "atualizacao_indicador_alfabetizacao",
        "id_municipio": id_municipio,
        "sigla_uf": sigla_uf,
        "nome_municipio": nome,
        "ano": datetime.now(timezone.utc).year,
        "percentual_alfabetizado": round(random.uniform(55.0, 95.0), 2),
        "amostra_avaliada": random.randint(50, 5000),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def send_event(client, event: dict) -> None:
    client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=(json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"),
        PartitionKey=event["id_municipio"],
    )
    log.info("Evento enviado: %s (%s)", event["event_id"], event["nome_municipio"])


def run(duration_seconds: int, interval_seconds: float) -> None:
    client = _kinesis_client()
    deadline = time.time() + duration_seconds
    while time.time() < deadline:
        send_event(client, build_event())
        time.sleep(interval_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=60, help="duracao total em segundos")
    parser.add_argument("--interval", type=float, default=5.0, help="intervalo entre eventos em segundos")
    args = parser.parse_args()
    run(args.duration, args.interval)
