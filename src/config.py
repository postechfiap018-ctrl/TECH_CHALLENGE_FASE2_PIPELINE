"""
Configuracao central do pipeline. Ajuste os valores abaixo antes de rodar
qualquer script (local, Glue ou notebook/Colab).
"""
import os

# ---------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")

# Bucket unico do data lake, com prefixos bronze/silver/gold (medalhao).
# Nomes de bucket S3 sao globais -> troque "aline" pelo seu sufixo se o nome
# ja estiver em uso por outra conta AWS.
DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET", "tech-challenge-alfabetizacao-aline")

BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"
ATHENA_RESULTS_PREFIX = "athena-results"
GLUE_SCRIPTS_PREFIX = "glue-scripts"

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "alfabetizacao_db")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "tech-challenge-alfabetizacao")

KINESIS_STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "alfabetizacao-streaming")

GLUE_ROLE_NAME = "TechChallengeAlfabetizacao-GlueRole"
LAMBDA_ROLE_NAME = "TechChallengeAlfabetizacao-LambdaRole"

GLUE_SILVER_JOB_NAME = "alfabetizacao-silver-job"
GLUE_GOLD_JOB_NAME = "alfabetizacao-gold-job"

LAMBDA_STREAMING_CONSUMER_NAME = "alfabetizacao-streaming-consumer"
LAMBDA_TRIGGER_GLUE_NAME = "alfabetizacao-trigger-glue-silver"

# ---------------------------------------------------------------------------
# GCP / BigQuery (Base dos Dados)
# ---------------------------------------------------------------------------
# Projeto de BILLING no GCP (o dataset publico "basedosdados" nao cobra
# armazenamento, mas a consulta precisa de um projeto com billing ativo
# para computar os bytes escaneados).
GCP_BILLING_PROJECT = os.environ.get("GCP_BILLING_PROJECT", "techchallenge-505723")

# Caminho do JSON da service account. Em ambiente local, aponte para o
# arquivo baixado do IAM do GCP. No Colab, faca upload do arquivo e ajuste
# o caminho para "/content/<nome-do-arquivo>.json".
GCP_SERVICE_ACCOUNT_JSON = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")

# ---------------------------------------------------------------------------
# Tabelas de origem na Base dos Dados (BigQuery publico)
#
# Confirmado em 2026-08-22 via a API GraphQL da Base dos Dados
# (backend.basedosdados.org/graphql), a partir do link exato que o PDF do
# desafio aponta para o dataset "Avaliacao da Alfabetizacao"
# (https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72).
# Esse dataset tem uma tabela para cada entidade pedida no enunciado (UF,
# Municipio, Meta Alfabetizacao Brasil/UF/Municipio, Alunos).
#
# "uf"/"municipio" abaixo sao a dimensao TERRITORIAL (nome, sigla_uf) do
# dataset publico de diretorios geograficos -- usada so para enriquecer os
# resultados com nome do municipio/UF. Os RESULTADOS e METAS de
# alfabetizacao propriamente ditos vem do dataset br_inep_avaliacao_alfabetizacao.
# ---------------------------------------------------------------------------
SOURCE_TABLES = {
    # Dimensao territorial (nome, sigla_uf) -- enriquecimento, nao e o
    # indicador em si.
    "uf": "basedosdados.br_bd_diretorios_brasil.uf",
    "municipio": "basedosdados.br_bd_diretorios_brasil.municipio",
    # Resultado realizado do indicador (taxa_alfabetizacao) + metas --
    # dataset br_inep_avaliacao_alfabetizacao, entidades do enunciado.
    "uf_resultado_alfabetizacao": "basedosdados.br_inep_avaliacao_alfabetizacao.uf",
    "municipio_resultado_alfabetizacao": "basedosdados.br_inep_avaliacao_alfabetizacao.municipio",
    "meta_alfabetizacao_brasil": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
    "alunos": "basedosdados.br_inep_avaliacao_alfabetizacao.alunos",
}

# Colunas com a trajetoria de metas (2024-2030) nas tabelas meta_alfabetizacao_*.
# O Glue Job Silver despivota essas colunas (wide -> long) para comparar cada
# ano com a meta definida para aquele mesmo ano.
META_YEAR_COLUMNS = [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]

# Colunas-chave usadas para join/validacao de integridade entre as tabelas.
KEY_COLUMNS = {
    "uf": ["sigla_uf"],
    "municipio": ["id_municipio"],
    "uf_resultado_alfabetizacao": ["sigla_uf", "ano", "rede", "serie"],
    "municipio_resultado_alfabetizacao": ["id_municipio", "ano", "rede", "serie"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_uf": ["sigla_uf", "ano", "rede"],
    "meta_alfabetizacao_municipio": ["id_municipio", "ano", "rede"],
    "alunos": ["id_aluno", "ano"],
}


def s3_path(prefix: str, entity: str, extra: str = "") -> str:
    """Monta o caminho S3 padronizado (particionado) para uma entidade."""
    parts = [f"s3://{DATALAKE_BUCKET}", prefix, entity]
    if extra:
        parts.append(extra)
    return "/".join(parts)
