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
# IMPORTANTE: os table_id abaixo sao PLACEHOLDERS. A Base dos Dados e um
# catalogo dinamico (SPA + GraphQL) e os nomes exatos de dataset/tabela
# mudam por catalogo. Antes de rodar a extracao:
#   1. Acesse https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72
#      (dataset "Avaliacao da Alfabetizacao") e, para as tabelas de UF e
#      Municipio, o dataset "Diretorios Brasileiros de Geografia".
#   2. Para cada tabela, clique em "Acessar dados" > aba "BigQuery" -> copie
#      o "table_id" completo (formato basedosdados.<dataset>.<tabela>).
#   3. Cole o valor correspondente abaixo.
# ---------------------------------------------------------------------------
SOURCE_TABLES = {
    "uf": "basedosdados.br_bd_diretorios_brasil.uf",
    "municipio": "basedosdados.br_bd_diretorios_brasil.municipio",
    "meta_alfabetizacao_brasil": "TODO_preencher_apos_consultar_basedosdados",
    "meta_alfabetizacao_uf": "TODO_preencher_apos_consultar_basedosdados",
    "meta_alfabetizacao_municipio": "TODO_preencher_apos_consultar_basedosdados",
    "dados_alunos_indicador": "TODO_preencher_apos_consultar_basedosdados",
}

# Colunas-chave usadas para join/validacao de integridade entre as tabelas.
KEY_COLUMNS = {
    "uf": ["sigla_uf"],
    "municipio": ["id_municipio"],
    "meta_alfabetizacao_brasil": ["ano"],
    "meta_alfabetizacao_uf": ["sigla_uf", "ano"],
    "meta_alfabetizacao_municipio": ["id_municipio", "ano"],
    "dados_alunos_indicador": ["id_municipio", "ano"],
}


def s3_path(prefix: str, entity: str, extra: str = "") -> str:
    """Monta o caminho S3 padronizado (particionado) para uma entidade."""
    parts = [f"s3://{DATALAKE_BUCKET}", prefix, entity]
    if extra:
        parts.append(extra)
    return "/".join(parts)
