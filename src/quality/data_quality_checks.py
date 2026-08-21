"""
Checks de qualidade de dados, usados na fronteira Bronze -> Silver e
Silver -> Gold. Puro pandas para poder rodar local, em notebook/Colab
ou dentro de um Glue Python Shell job (sem precisar de Spark).

Uso:
    from src.quality.data_quality_checks import run_quality_report
    report = run_quality_report(df, key_columns=["id_municipio", "ano"], entity="municipio")
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class QualityReport:
    entity: str
    generated_at: str
    row_count: int
    duplicate_rows: int
    duplicate_keys: int
    null_counts: dict[str, int]
    null_pct: dict[str, float]
    key_columns: list[str]
    passed: bool
    issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


def run_quality_report(df, key_columns: list[str], entity: str,
                        null_threshold_pct: float = 30.0) -> QualityReport:
    """Verifica duplicidade, nulos e chaves. Nao lanca excecao: registra
    'issues' e marca passed=False para o caller decidir o que fazer
    (ex.: barrar a promocao Bronze->Silver ou apenas alertar)."""
    issues: list[str] = []

    row_count = len(df)

    duplicate_rows = int(df.duplicated().sum())
    if duplicate_rows > 0:
        issues.append(f"{duplicate_rows} linhas totalmente duplicadas")

    missing_keys = [c for c in key_columns if c not in df.columns]
    if missing_keys:
        issues.append(f"colunas-chave ausentes no dataframe: {missing_keys}")
        duplicate_keys = 0
    else:
        duplicate_keys = int(df.duplicated(subset=key_columns).sum())
        if duplicate_keys > 0:
            issues.append(f"{duplicate_keys} linhas com chave duplicada {key_columns}")
        null_key_rows = int(df[key_columns].isnull().any(axis=1).sum())
        if null_key_rows > 0:
            issues.append(f"{null_key_rows} linhas com chave nula {key_columns}")

    null_counts = df.isnull().sum().to_dict()
    null_pct = {
        col: round((cnt / row_count) * 100, 2) if row_count else 0.0
        for col, cnt in null_counts.items()
    }
    high_null_cols = [c for c, pct in null_pct.items() if pct > null_threshold_pct]
    if high_null_cols:
        issues.append(
            f"colunas acima do limite de {null_threshold_pct}% de nulos: {high_null_cols}"
        )

    passed = len(issues) == 0

    return QualityReport(
        entity=entity,
        generated_at=datetime.now(timezone.utc).isoformat(),
        row_count=row_count,
        duplicate_rows=duplicate_rows,
        duplicate_keys=duplicate_keys,
        null_counts={k: int(v) for k, v in null_counts.items()},
        null_pct=null_pct,
        key_columns=key_columns,
        passed=passed,
        issues=issues,
    )


def check_referential_integrity(child_df, parent_df, child_key: str,
                                 parent_key: str) -> dict[str, Any]:
    """Confere se todo valor de child_key em child_df existe em parent_key
    de parent_df (ex.: todo id_municipio em 'dados_alunos' existe em
    'municipio'). Usado na integracao Silver."""
    child_values = set(child_df[child_key].dropna().unique())
    parent_values = set(parent_df[parent_key].dropna().unique())
    orphans = child_values - parent_values
    return {
        "child_key": child_key,
        "parent_key": parent_key,
        "orphan_count": len(orphans),
        "orphan_sample": list(orphans)[:20],
        "passed": len(orphans) == 0,
    }


def save_report_to_s3(report: QualityReport, bucket: str, key: str) -> None:
    """Grava o relatorio de qualidade no S3 (camada de governanca), para
    auditoria e para os alarmes de CloudWatch/Glue consultarem depois."""
    import boto3

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=report.to_json().encode("utf-8"),
        ContentType="application/json",
    )
