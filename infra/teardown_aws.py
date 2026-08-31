"""
script de teardown (FinOps - custo zero apos o uso)

Uso:
    python -m infra.teardown_aws --yes
"""
from __future__ import annotations

import argparse

import boto3
from botocore.exceptions import ClientError

from src.config import (
    ATHENA_WORKGROUP,
    AWS_REGION,
    DATALAKE_BUCKET,
    GLUE_DATABASE,
    GLUE_GOLD_JOB_NAME,
    GLUE_ROLE_NAME,
    GLUE_SILVER_JOB_NAME,
    KINESIS_STREAM_NAME,
    LAMBDA_ROLE_NAME,
    LAMBDA_STREAMING_CONSUMER_NAME,
    LAMBDA_TRIGGER_GLUE_NAME,
)

session = boto3.session.Session(region_name=AWS_REGION)
s3 = session.client("s3")
iam = session.client("iam")
glue = session.client("glue")
kinesis = session.client("kinesis")
lambda_client = session.client("lambda")
athena = session.client("athena")
cloudwatch = session.client("cloudwatch")


def log(msg: str) -> None:
    print(f"[teardown] {msg}")


def ignore_not_found(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in ("ResourceNotFoundException", "EntityNotFoundException", "NoSuchEntity"):
            log(f"aviso: {exc}")


def delete_role(role_name: str) -> None:
    try:
        for pol in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=pol)
        for pol in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=pol["PolicyArn"])
        iam.delete_role(RoleName=role_name)
        log(f"role {role_name} removida")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise


def empty_and_delete_bucket() -> None:
    paginator = s3.get_paginator("list_object_versions")
    try:
        for page in paginator.paginate(Bucket=DATALAKE_BUCKET):
            objs = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("Versions", [])]
            objs += [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("DeleteMarkers", [])]
            if objs:
                s3.delete_objects(Bucket=DATALAKE_BUCKET, Delete={"Objects": objs})
        s3.delete_bucket(Bucket=DATALAKE_BUCKET)
        log(f"bucket {DATALAKE_BUCKET} esvaziado e removido")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchBucket":
            raise


def main(confirm: bool) -> None:
    if not confirm:
        log("modo dry-run (nada sera removido). Rode com --yes para executar de fato.")

    for fn in (LAMBDA_STREAMING_CONSUMER_NAME, LAMBDA_TRIGGER_GLUE_NAME):
        if confirm:
            ignore_not_found(lambda_client.delete_function, FunctionName=fn)
        log(f"lambda {fn} -> {'removida' if confirm else 'seria removida'}")

    if confirm:
        ignore_not_found(kinesis.delete_stream, StreamName=KINESIS_STREAM_NAME, EnforceConsumerDeletion=True)
    log(f"kinesis stream {KINESIS_STREAM_NAME} -> {'removido' if confirm else 'seria removido'}")

    for job in (GLUE_SILVER_JOB_NAME, GLUE_GOLD_JOB_NAME):
        if confirm:
            ignore_not_found(glue.delete_job, JobName=job)
        log(f"glue job {job} -> {'removido' if confirm else 'seria removido'}")

    if confirm:
        ignore_not_found(glue.delete_database, Name=GLUE_DATABASE)
    log(f"glue database {GLUE_DATABASE} -> {'removido' if confirm else 'seria removido'}")

    if confirm:
        ignore_not_found(athena.delete_work_group, WorkGroup=ATHENA_WORKGROUP, RecursiveDeleteOption=True)
    log(f"athena workgroup {ATHENA_WORKGROUP} -> {'removido' if confirm else 'seria removido'}")

    if confirm:
        delete_role(GLUE_ROLE_NAME)
        delete_role(LAMBDA_ROLE_NAME)
    else:
        log(f"roles {GLUE_ROLE_NAME}/{LAMBDA_ROLE_NAME} -> seriam removidas")

    if confirm:
        empty_and_delete_bucket()
    else:
        log(f"bucket {DATALAKE_BUCKET} -> seria esvaziado e removido (DESTRUTIVO)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="confirma a remocao (destrutivo)")
    args = parser.parse_args()
    main(confirm=args.yes)
