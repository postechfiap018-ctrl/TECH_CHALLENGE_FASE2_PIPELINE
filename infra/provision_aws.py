"""
Provisionamento da infraestrutura AWS do Tech Challenge Fase 2
(Pipeline Hibrido - Alfabetizacao), via boto3. Idempotente: pode rodar
mais de uma vez sem duplicar recursos.

Cria:
  - Bucket S3 (data lake: bronze/silver/gold/athena-results/glue-scripts),
    com versionamento e lifecycle rules (FinOps).
  - IAM roles para Glue e Lambda.
  - Glue Database + 2 Glue Jobs (silver, gold).
  - Kinesis Data Stream (on-demand, 1 stream) para a ingestao streaming.
  - 2 funcoes Lambda (streaming consumer + trigger do job Silver) e as
    integracoes de evento (Kinesis -> Lambda, S3 -> Lambda).
  - Athena Workgroup com limite de bytes escaneados por query (FinOps).
  - CloudWatch Log Group + alarme de erros da Lambda (monitoramento).

Pre-requisito: credenciais AWS validas configuradas (`aws configure` ou
variaveis de ambiente AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), com um
usuario IAM que tenha permissao para criar os recursos acima (ver README,
secao "Passo a passo: criar o usuario IAM").

Uso:
    python -m infra.provision_aws
"""
from __future__ import annotations

import io
import json
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from src.config import (
    ATHENA_RESULTS_PREFIX,
    ATHENA_WORKGROUP,
    AWS_REGION,
    BRONZE_PREFIX,
    DATALAKE_BUCKET,
    GLUE_DATABASE,
    GLUE_GOLD_JOB_NAME,
    GLUE_ROLE_NAME,
    GLUE_SCRIPTS_PREFIX,
    GLUE_SILVER_JOB_NAME,
    GOLD_PREFIX,
    KINESIS_STREAM_NAME,
    LAMBDA_ROLE_NAME,
    LAMBDA_STREAMING_CONSUMER_NAME,
    LAMBDA_TRIGGER_GLUE_NAME,
    SILVER_PREFIX,
)

ROOT = Path(__file__).resolve().parent.parent
IAM_POLICIES_DIR = ROOT / "infra" / "iam_policies"
TAGS = [{"Key": "Project", "Value": "tech-challenge-alfabetizacao"}]

session = boto3.session.Session(region_name=AWS_REGION)
sts = session.client("sts")
s3 = session.client("s3")
iam = session.client("iam")
glue = session.client("glue")
kinesis = session.client("kinesis")
lambda_client = session.client("lambda")
athena = session.client("athena")
logs = session.client("logs")
cloudwatch = session.client("cloudwatch")

ACCOUNT_ID = sts.get_caller_identity()["Account"]


def log(msg: str) -> None:
    print(f"[provision] {msg}")


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------
def ensure_bucket() -> None:
    try:
        s3.head_bucket(Bucket=DATALAKE_BUCKET)
        log(f"bucket {DATALAKE_BUCKET} ja existe")
    except ClientError:
        kwargs = {"Bucket": DATALAKE_BUCKET}
        if AWS_REGION != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": AWS_REGION}
        s3.create_bucket(**kwargs)
        log(f"bucket {DATALAKE_BUCKET} criado em {AWS_REGION}")

    s3.put_bucket_versioning(
        Bucket=DATALAKE_BUCKET, VersioningConfiguration={"Status": "Enabled"}
    )
    s3.put_bucket_tagging(Bucket=DATALAKE_BUCKET, Tagging={"TagSet": TAGS})

    # FinOps: dados brutos/streaming migram para storage mais barato e
    # expiram depois de um tempo; resultados de query do Athena (efemeros)
    # expiram rapido; versoes antigas nao ficam acumulando custo para sempre.
    s3.put_bucket_lifecycle_configuration(
        Bucket=DATALAKE_BUCKET,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "bronze-tiering",
                    "Filter": {"Prefix": f"{BRONZE_PREFIX}/"},
                    "Status": "Enabled",
                    "Transitions": [
                        {"Days": 30, "StorageClass": "STANDARD_IA"},
                        {"Days": 90, "StorageClass": "GLACIER"},
                    ],
                },
                {
                    "ID": "athena-results-expire",
                    "Filter": {"Prefix": f"{ATHENA_RESULTS_PREFIX}/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 7},
                },
                {
                    "ID": "noncurrent-versions-expire",
                    "Filter": {"Prefix": ""},
                    "Status": "Enabled",
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                },
            ]
        },
    )
    log("versionamento + lifecycle (FinOps) configurados")


# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------
def _render_policy(filename: str) -> str:
    text = (IAM_POLICIES_DIR / filename).read_text(encoding="utf-8")
    text = text.replace("{BUCKET}", DATALAKE_BUCKET)
    text = text.replace("{ACCOUNT_ID}", ACCOUNT_ID)
    text = text.replace("{KINESIS_STREAM_NAME}", KINESIS_STREAM_NAME)
    text = text.replace("{GLUE_SILVER_JOB_NAME}", GLUE_SILVER_JOB_NAME)
    return text


def ensure_role(role_name: str, trust_file: str, permissions_file: str,
                 managed_arns: list[str] | None = None) -> str:
    try:
        role = iam.get_role(RoleName=role_name)
        log(f"role {role_name} ja existe")
    except ClientError:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=_render_policy(trust_file),
            Tags=TAGS,
        )
        log(f"role {role_name} criada")

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=f"{role_name}-inline",
        PolicyDocument=_render_policy(permissions_file),
    )
    for arn in managed_arns or []:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)

    return role["Role"]["Arn"]


# ---------------------------------------------------------------------------
# Glue
# ---------------------------------------------------------------------------
def ensure_glue_database() -> None:
    try:
        glue.get_database(Name=GLUE_DATABASE)
        log(f"glue database {GLUE_DATABASE} ja existe")
    except ClientError:
        glue.create_database(DatabaseInput={"Name": GLUE_DATABASE})
        log(f"glue database {GLUE_DATABASE} criado")


def upload_glue_scripts() -> dict[str, str]:
    scripts = {
        "silver": ROOT / "src" / "silver" / "glue_silver_job.py",
        "gold": ROOT / "src" / "gold" / "glue_gold_job.py",
    }
    s3_paths = {}
    for name, path in scripts.items():
        key = f"{GLUE_SCRIPTS_PREFIX}/{path.name}"
        s3.upload_file(str(path), DATALAKE_BUCKET, key)
        s3_paths[name] = f"s3://{DATALAKE_BUCKET}/{key}"
        log(f"script {name} enviado para {s3_paths[name]}")
    return s3_paths


def ensure_glue_job(job_name: str, script_s3_path: str, role_arn: str) -> None:
    default_args = {
        "--job-language": "python",
        "--DATALAKE_BUCKET": DATALAKE_BUCKET,
        "--GLUE_DATABASE": GLUE_DATABASE,
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--job-bookmark-option": "job-bookmark-enable",  # so processa dado novo (FinOps)
    }
    config = dict(
        Role=role_arn,
        Command={"Name": "glueetl", "ScriptLocation": script_s3_path, "PythonVersion": "3"},
        DefaultArguments=default_args,
        GlueVersion="4.0",
        WorkerType="G.1X",
        NumberOfWorkers=2,  # menor tamanho pratico (FinOps); aumente se o volume crescer
        Timeout=30,
        Tags={t["Key"]: t["Value"] for t in TAGS},
    )
    try:
        glue.get_job(JobName=job_name)
        glue.update_job(JobName=job_name, JobUpdate=config)
        log(f"glue job {job_name} atualizado")
    except ClientError:
        glue.create_job(Name=job_name, **config)
        log(f"glue job {job_name} criado")


# ---------------------------------------------------------------------------
# Kinesis
# ---------------------------------------------------------------------------
def ensure_kinesis_stream() -> str:
    try:
        desc = kinesis.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
        log(f"kinesis stream {KINESIS_STREAM_NAME} ja existe")
    except ClientError:
        # On-demand: sem provisionar shards fixos, paga por uso (FinOps).
        kinesis.create_stream(StreamName=KINESIS_STREAM_NAME, StreamModeDetails={"StreamMode": "ON_DEMAND"})
        log(f"kinesis stream {KINESIS_STREAM_NAME} criado, aguardando ficar ACTIVE...")
        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=KINESIS_STREAM_NAME)
        desc = kinesis.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
    return desc["StreamDescriptionSummary"]["StreamARN"]


# ---------------------------------------------------------------------------
# Lambda
# ---------------------------------------------------------------------------
def _zip_handler(handler_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(handler_dir / "handler.py", arcname="handler.py")
    buf.seek(0)
    return buf.read()


def ensure_lambda(function_name: str, handler_dir: Path, role_arn: str,
                   env: dict[str, str], timeout: int = 30) -> str:
    zip_bytes = _zip_handler(handler_dir)
    try:
        lambda_client.get_function(FunctionName=function_name)
        lambda_client.update_function_code(FunctionName=function_name, ZipFile=zip_bytes)
        lambda_client.update_function_configuration(
            FunctionName=function_name, Environment={"Variables": env}, Timeout=timeout
        )
        log(f"lambda {function_name} atualizada")
    except ClientError:
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="handler.handler",
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": env},
            Timeout=timeout,
            MemorySize=256,  # menor memoria pratica (FinOps)
            Tags={t["Key"]: t["Value"] for t in TAGS},
        )
        log(f"lambda {function_name} criada")

    return lambda_client.get_function(FunctionName=function_name)["Configuration"]["FunctionArn"]


def ensure_kinesis_event_source_mapping(function_name: str, stream_arn: str) -> None:
    existing = lambda_client.list_event_source_mappings(FunctionName=function_name)["EventSourceMappings"]
    if any(m["EventSourceArn"] == stream_arn for m in existing):
        log(f"event source mapping Kinesis -> {function_name} ja existe")
        return
    lambda_client.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_name,
        StartingPosition="LATEST",
        BatchSize=50,
        MaximumBatchingWindowInSeconds=10,
    )
    log(f"event source mapping Kinesis -> {function_name} criado")


def ensure_s3_trigger(function_name: str, function_arn: str) -> None:
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId="AllowS3Invoke",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{DATALAKE_BUCKET}",
            SourceAccount=ACCOUNT_ID,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceConflictException":
            raise

    s3.put_bucket_notification_configuration(
        Bucket=DATALAKE_BUCKET,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn": function_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": f"{BRONZE_PREFIX}/"}]}},
                }
            ]
        },
    )
    log(f"trigger S3 ({BRONZE_PREFIX}/*) -> {function_name} configurado")


# ---------------------------------------------------------------------------
# Athena
# ---------------------------------------------------------------------------
def ensure_athena_workgroup() -> None:
    try:
        athena.get_work_group(WorkGroup=ATHENA_WORKGROUP)
        log(f"athena workgroup {ATHENA_WORKGROUP} ja existe")
        return
    except ClientError:
        pass

    athena.create_work_group(
        Name=ATHENA_WORKGROUP,
        Configuration={
            "ResultConfiguration": {
                "OutputLocation": f"s3://{DATALAKE_BUCKET}/{ATHENA_RESULTS_PREFIX}/"
            },
            # FinOps: barra queries que escaneiam mais de 1 GB (evita custo
            # acidental de uma query sem filtro/particao em tabela grande).
            "BytesScannedCutoffPerQuery": 1_000_000_000,
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
        },
        Tags=TAGS,
    )
    log(f"athena workgroup {ATHENA_WORKGROUP} criado (limite 1GB escaneado/query)")


# ---------------------------------------------------------------------------
# CloudWatch (monitoramento minimo)
# ---------------------------------------------------------------------------
def ensure_monitoring() -> None:
    for fn in (LAMBDA_STREAMING_CONSUMER_NAME, LAMBDA_TRIGGER_GLUE_NAME):
        try:
            cloudwatch.put_metric_alarm(
                AlarmName=f"{fn}-errors",
                MetricName="Errors",
                Namespace="AWS/Lambda",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                Statistic="Sum",
                Period=300,
                EvaluationPeriods=1,
                Threshold=1,
                ComparisonOperator="GreaterThanOrEqualToThreshold",
                TreatMissingData="notBreaching",
                AlarmDescription=f"Alerta de falha de ingestao em {fn}",
            )
        except ClientError as exc:
            log(f"aviso: nao foi possivel criar alarme para {fn}: {exc}")
    log("alarmes de CloudWatch (falhas de Lambda) configurados")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    log(f"conta AWS: {ACCOUNT_ID} | regiao: {AWS_REGION} | bucket: {DATALAKE_BUCKET}")

    ensure_bucket()
    ensure_glue_database()

    glue_role_arn = ensure_role(
        GLUE_ROLE_NAME, "glue_role_trust.json", "glue_role_permissions.json"
    )
    lambda_role_arn = ensure_role(
        LAMBDA_ROLE_NAME, "lambda_role_trust.json", "lambda_role_permissions.json"
    )

    script_paths = upload_glue_scripts()
    ensure_glue_job(GLUE_SILVER_JOB_NAME, script_paths["silver"], glue_role_arn)
    ensure_glue_job(GLUE_GOLD_JOB_NAME, script_paths["gold"], glue_role_arn)

    stream_arn = ensure_kinesis_stream()

    consumer_arn = ensure_lambda(
        LAMBDA_STREAMING_CONSUMER_NAME,
        ROOT / "src" / "lambda" / "streaming_consumer",
        lambda_role_arn,
        env={"DATALAKE_BUCKET": DATALAKE_BUCKET},
    )
    ensure_kinesis_event_source_mapping(LAMBDA_STREAMING_CONSUMER_NAME, stream_arn)

    trigger_arn = ensure_lambda(
        LAMBDA_TRIGGER_GLUE_NAME,
        ROOT / "src" / "lambda" / "trigger_glue",
        lambda_role_arn,
        env={"GLUE_SILVER_JOB_NAME": GLUE_SILVER_JOB_NAME},
    )
    ensure_s3_trigger(LAMBDA_TRIGGER_GLUE_NAME, trigger_arn)

    ensure_athena_workgroup()
    ensure_monitoring()

    log("provisionamento concluido.")
    log(f"proximo passo: rode a extracao batch (src/bronze/extract_batch_bigquery.py) "
        f"para popular {BRONZE_PREFIX}/ e disparar a pipeline.")


if __name__ == "__main__":
    main()
