"""Audit helpers for paid commission spreadsheets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from commission_tool.core.eligibility import normalize_document_padded
from commission_tool.core.formatting import parse_br_number, parse_commission_percent_points, parse_percent_points


MONEY_TOLERANCE = 0.05
PERCENT_TOLERANCE = 0.01

REPORT_COLUMN_ALIASES = {
    "Cliente": "Nome do Cliente",
    "% Comissão NF": "% Comissão Fat.",
    "% Comissao NF": "% Comissão Fat.",
    "Valor Comissão NF": "Valor Comissão Fat.",
    "Valor Comissao NF": "Valor Comissão Fat.",
    "Meta Margem": "Meta de Margem",
}

REQUIRED_AUDIT_COLUMNS = [
    "Modelo",
    "Nro Chassi",
    "Nro Documento",
    "Receita Bruta",
    "% Comissão Fat.",
    "Valor Comissão Fat.",
    "% Comissão Margem",
    "Valor Comissão Margem",
    "Valor Comissão Total",
]

FAT_RATE_PERCENT_COLUMNS = [
    "Percentual",
    "% Comissão Fat.",
    "% Comissão Fat",
    "% Comissao Fat.",
    "% Comissao Fat",
    "% Comissão NF",
    "% Comissao NF",
    "percentual",
    "percentual_comissao_fat",
]

MARGIN_RATE_PERCENT_COLUMNS = [
    "Percentual",
    "% Comissão Margem",
    "% Comissão Margem.",
    "% Comissao Margem",
    "% Comissao Margem.",
    "percentual",
    "percentual_comissao_margem",
]


@dataclass
class PaidCommissionAuditResult:
    file_name: str
    dataframe: pd.DataFrame
    passed: bool
    row_count: int
    error_count: int
    warning_count: int
    total_commission: float
    issues: pd.DataFrame
    summary: dict[str, Any]


def normalize_model(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def sum_numeric_column(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns:
        return 0.0
    return float(df[column].apply(parse_br_number).pipe(pd.to_numeric, errors="coerce").sum())


def normalize_commission_report_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.rename(columns=REPORT_COLUMN_ALIASES).copy()
    if "Nro Documento" in normalized.columns:
        normalized["Nro Documento"] = normalized["Nro Documento"].apply(normalize_document_padded)
    if "Nro Chassi" in normalized.columns:
        normalized["Nro Chassi"] = normalized["Nro Chassi"].fillna("").astype(str).str.strip().str.upper()
    if "Modelo" in normalized.columns:
        normalized["Modelo"] = normalized["Modelo"].fillna("").astype(str).str.strip()
    return normalized


def normalize_commission_report_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = normalize_commission_report_columns(df)
    if "Filial" in normalized.columns:
        normalized = normalized[
            normalized["Filial"].notna()
            & normalized["Filial"].astype(str).str.strip().ne("")
            & ~normalized["Filial"].astype(str).str.strip().str.lower().isin(["nan"])
        ]
    return normalized.reset_index(drop=True)


def find_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    columns = list(df.columns)
    for candidate in candidates:
        if candidate in columns:
            return candidate

    lookup = {str(column).strip().lower(): column for column in columns}
    for candidate in candidates:
        match = lookup.get(candidate.strip().lower())
        if match is not None:
            return match

    if required:
        raise KeyError(f"Coluna não encontrada. Candidatas: {candidates}. Disponíveis: {columns}")
    return None


def build_rate_lookup(df: pd.DataFrame, percent_candidates: list[str]) -> dict[str, set[float]]:
    if df is None or df.empty:
        return {}

    modelo_col = find_column(df, ["Modelo", "modelo", "MODELO"])
    percent_col = find_column(df, percent_candidates)

    prepared = pd.DataFrame(
        {
            "modelo": df[modelo_col].apply(normalize_model),
            "percentual": df[percent_col].apply(parse_commission_percent_points),
        }
    )
    prepared = prepared[prepared["modelo"].ne("")]

    lookup: dict[str, set[float]] = {}
    for row in prepared.itertuples(index=False):
        lookup.setdefault(row.modelo, set()).add(float(row.percentual))
    return lookup


def build_extraction_key_set(df_machine: pd.DataFrame | None) -> set[tuple[str, str]]:
    if df_machine is None or df_machine.empty:
        return set()
    required = {"Nro Chassi", "Nro Documento"}
    if not required.issubset(df_machine.columns):
        return set()
    keys = set()
    for _, row in df_machine.iterrows():
        chassi = str(row.get("Nro Chassi", "") or "").strip().upper()
        document = normalize_document_padded(row.get("Nro Documento", ""))
        if chassi and document:
            keys.add((chassi, document))
    return keys


def value_matches_rule(value: float | None, allowed_values: set[float]) -> bool:
    if value is None or not allowed_values:
        return False
    return any(abs(float(value) - expected) <= PERCENT_TOLERANCE for expected in allowed_values)


def validate_paid_commission_file(
    file_name: str,
    df: pd.DataFrame,
    fat_rate_lookup: dict[str, set[float]],
    margin_rate_lookup: dict[str, set[float]],
    extraction_keys: set[tuple[str, str]],
) -> PaidCommissionAuditResult:
    loaded_row_count = len(df)
    loaded_column_count = len(df.columns)
    normalized = normalize_commission_report_df(df)
    loaded_revenue = sum_numeric_column(normalized, "Receita Bruta")
    loaded_total_commission = sum_numeric_column(normalized, "Valor Comissão Total")
    issues: list[dict[str, Any]] = []

    missing_columns = [column for column in REQUIRED_AUDIT_COLUMNS if column not in normalized.columns]
    for column in missing_columns:
        issues.append(
            {
                "Arquivo": file_name,
                "Linha": "",
                "Severidade": "Erro",
                "Regra": "Coluna obrigatória",
                "Detalhe": f"Coluna ausente: {column}",
            }
        )

    if missing_columns:
        return PaidCommissionAuditResult(
            file_name=file_name,
            dataframe=normalized,
            passed=False,
            row_count=len(normalized),
            error_count=len(issues),
            warning_count=0,
            total_commission=0.0,
            issues=pd.DataFrame(issues),
            summary={
                "missing_columns": missing_columns,
                "loaded_row_count": loaded_row_count,
                "loaded_column_count": loaded_column_count,
                "validated_row_count": len(normalized),
                "loaded_revenue": loaded_revenue,
                "loaded_total_commission": loaded_total_commission,
            },
        )

    work = normalized.copy()
    for column in [
        "Receita Bruta",
        "Valor Comissão Fat.",
        "Valor Comissão Margem",
        "Valor Comissão Total",
    ]:
        work[column] = work[column].apply(parse_br_number)
    for column in ["% Comissão Fat.", "% Comissão Margem"]:
        work[column] = work[column].apply(parse_commission_percent_points)

    duplicated_keys = work.duplicated(["Nro Chassi", "Nro Documento"], keep=False)
    for index, row in work[duplicated_keys].iterrows():
        issues.append(
            {
                "Arquivo": file_name,
                "Linha": int(index) + 2,
                "Severidade": "Erro",
                "Regra": "Chave única",
                "Detalhe": f"Chassi/documento duplicado: {row['Nro Chassi']} / {row['Nro Documento']}",
            }
        )

    if not extraction_keys:
        issues.append(
            {
                "Arquivo": file_name,
                "Linha": "",
                "Severidade": "Erro",
                "Regra": "Extração atual",
                "Detalhe": "Nenhuma extração atual carregada para validar Chassi + Nro Documento.",
            }
        )
    else:
        for index, row in work.iterrows():
            key = (str(row["Nro Chassi"]).strip().upper(), normalize_document_padded(row["Nro Documento"]))
            if key not in extraction_keys:
                issues.append(
                    {
                        "Arquivo": file_name,
                        "Linha": int(index) + 2,
                        "Severidade": "Erro",
                        "Regra": "Chave na extração",
                        "Detalhe": f"Chassi/documento não encontrado na extração atual: {key[0]} / {key[1]}",
                    }
                )

    rows_6125j = []
    for index, row in work.iterrows():
        modelo = normalize_model(row["Modelo"])
        receita = safe_float(row["Receita Bruta"])
        fat_percent = safe_float(row["% Comissão Fat."]) or 0.0
        margin_percent = safe_float(row["% Comissão Margem"]) or 0.0
        fat_value = safe_float(row["Valor Comissão Fat."])
        margin_value = safe_float(row["Valor Comissão Margem"])
        total_value = safe_float(row["Valor Comissão Total"])

        allowed_fat = fat_rate_lookup.get(modelo, set())
        allowed_margin = margin_rate_lookup.get(modelo, set())

        if modelo == "6125J":
            rows_6125j.append(fat_percent)

        if not value_matches_rule(fat_percent, allowed_fat):
            issues.append(
                {
                    "Arquivo": file_name,
                    "Linha": int(index) + 2,
                    "Severidade": "Erro",
                    "Regra": "% Comissão Fat.",
                    "Detalhe": f"Modelo {modelo}: arquivo={fat_percent:.6f}, regra(s)={sorted(allowed_fat)}",
                }
            )

        if receita not in [None, 0] and fat_value is not None:
            calculated_fat_percent = (fat_value / receita) * 100
            if not value_matches_rule(calculated_fat_percent, allowed_fat):
                issues.append(
                    {
                        "Arquivo": file_name,
                        "Linha": int(index) + 2,
                        "Severidade": "Erro",
                        "Regra": "Valor Comissão Fat. / Receita Bruta",
                        "Detalhe": (
                            f"Modelo {modelo}: calculado={calculated_fat_percent:.6f}, "
                            f"regra(s)={sorted(allowed_fat)}"
                        ),
                    }
                )

        if not value_matches_rule(margin_percent, allowed_margin):
            issues.append(
                {
                    "Arquivo": file_name,
                    "Linha": int(index) + 2,
                    "Severidade": "Erro",
                    "Regra": "% Comissão Margem",
                    "Detalhe": f"Modelo {modelo}: arquivo={margin_percent:.6f}, regra(s)={sorted(allowed_margin)}",
                }
            )

        if receita not in [None, 0] and margin_value is not None:
            calculated_margin_percent = (margin_value / receita) * 100
            if not value_matches_rule(calculated_margin_percent, allowed_margin):
                issues.append(
                    {
                        "Arquivo": file_name,
                        "Linha": int(index) + 2,
                        "Severidade": "Erro",
                        "Regra": "Valor Comissão Margem / Receita Bruta",
                        "Detalhe": (
                            f"Modelo {modelo}: calculado={calculated_margin_percent:.6f}, "
                            f"regra(s)={sorted(allowed_margin)}"
                        ),
                    }
                )

        expected_fat_value = 0.0 if receita is None else receita * fat_percent / 100
        expected_margin_value = 0.0 if receita is None else receita * margin_percent / 100
        expected_total = expected_fat_value + expected_margin_value
        if total_value is None or abs(total_value - expected_total) > MONEY_TOLERANCE:
            issues.append(
                {
                    "Arquivo": file_name,
                    "Linha": int(index) + 2,
                    "Severidade": "Erro",
                    "Regra": "Valor Comissão Total",
                    "Detalhe": (
                        f"arquivo={total_value}, recalculado={expected_total:.2f} "
                        f"(fat={expected_fat_value:.2f}, margem={expected_margin_value:.2f})"
                    ),
                }
            )

    if rows_6125j:
        allowed_6125j = sorted(fat_rate_lookup.get("6125J", set()))
        used_6125j = sorted(set(float(value) for value in rows_6125j))
        if not all(value_matches_rule(value, set(allowed_6125j)) for value in used_6125j):
            issues.append(
                {
                    "Arquivo": file_name,
                    "Linha": "",
                    "Severidade": "Erro",
                    "Regra": "Modelo 6125J",
                    "Detalhe": f"percentual usado={used_6125j}, regra faturamento={allowed_6125j}",
                }
            )
    else:
        issues.append(
            {
                "Arquivo": file_name,
                "Linha": "",
                "Severidade": "Aviso",
                "Regra": "Modelo 6125J",
                "Detalhe": "Modelo 6125J não encontrado no arquivo.",
            }
        )

    issues_df = pd.DataFrame(issues)
    error_count = 0 if issues_df.empty else int((issues_df["Severidade"] == "Erro").sum())
    warning_count = 0 if issues_df.empty else int((issues_df["Severidade"] == "Aviso").sum())
    total_commission = pd.to_numeric(work["Valor Comissão Total"], errors="coerce").sum()

    return PaidCommissionAuditResult(
        file_name=file_name,
        dataframe=normalized,
        passed=error_count == 0,
        row_count=len(work),
        error_count=error_count,
        warning_count=warning_count,
        total_commission=float(total_commission),
        issues=issues_df,
        summary={
            "loaded_row_count": loaded_row_count,
            "loaded_column_count": loaded_column_count,
            "validated_row_count": len(work),
            "loaded_revenue": loaded_revenue,
            "loaded_total_commission": loaded_total_commission,
            "used_6125j_percentages": sorted(set(float(value) for value in rows_6125j)),
            "expected_6125j_percentages": sorted(fat_rate_lookup.get("6125J", set())),
        },
    )
