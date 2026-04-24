"""Reporting helpers for commission summaries."""

from __future__ import annotations

import unicodedata
from typing import Any

import pandas as pd


ANALYTIC_REPORT_COLUMN_MAP = [
    ("filial", "Filial"),
    ("nro_documento", "Nro Documento"),
    ("nro_chassi", "Nro Chassi"),
    ("nome_cliente", "Nome do Cliente"),
    ("cen", "CEN"),
    ("classificacao_venda", "Classificação Venda"),
    ("receita_bruta", "Receita Bruta"),
    ("valor_comissao_fat", "Valor Comissão Fat."),
    ("margem_rs", "Margem R$"),
    ("perc_margem_direta", "% Margem Direta"),
    ("valor_incentivo", "Valor Incentivo"),
    ("margem_incentivos_rs", "Margem + Incentivos R$"),
    ("meta_margem", "Meta de Margem"),
    ("perc_margem_bruta", "% Margem Bruta"),
    ("valor_comissao_margem", "Valor Comissão Margem"),
    ("valor_comissao_total", "Valor Comissão Total"),
]


def _normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char)).upper()


def _normalize_code(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _prepare_manager_relations(manager_relations: pd.DataFrame | None) -> pd.DataFrame:
    if manager_relations is None or manager_relations.empty:
        return pd.DataFrame()

    relations = manager_relations.copy()
    relations["__cod_vendedor"] = relations.get("cod_vendedor", pd.Series(dtype=object)).apply(_normalize_code)
    relations["__gerente"] = relations.get("gerente", pd.Series(dtype=object)).apply(_normalize_text)
    relations["__vendedor"] = relations.get("vendedor", pd.Series(dtype=object)).apply(_normalize_text)
    relations["__gerente_display"] = relations.get("gerente", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    relations["__vendedor_display"] = relations.get("vendedor", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    return relations[
        relations["__cod_vendedor"].ne("")
        & relations["__gerente"].ne("")
        & relations["__vendedor_display"].ne("")
    ].copy()


def _prepare_paid_commissions_for_period(
    paid_commissions: pd.DataFrame | None,
    period_label: str,
) -> pd.DataFrame:
    if paid_commissions is None or paid_commissions.empty:
        return pd.DataFrame()

    paid = paid_commissions.copy()
    paid = paid[
        paid.get("mes_ano_comissao", pd.Series(dtype=object)).fillna("").astype(str).eq(str(period_label))
    ].copy()
    if paid.empty:
        return paid

    paid["__cod_vendedor"] = paid.get("cod_vendedor", pd.Series(dtype=object)).apply(_normalize_code)
    paid["__classificacao"] = paid.get("classificacao_venda", pd.Series(dtype=object)).apply(_normalize_text)
    paid["__cen"] = paid.get("cen", pd.Series(dtype=object)).apply(_normalize_text)
    paid["__filial"] = paid.get("filial", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    numeric_columns = [
        "receita_bruta",
        "valor_comissao_fat",
        "margem_rs",
        "perc_margem_direta",
        "valor_incentivo",
        "margem_incentivos_rs",
        "meta_margem",
        "perc_margem_bruta",
        "valor_comissao_margem",
        "receita_bruta_incentivos_rs",
    ]
    for column in numeric_columns:
        paid[column] = pd.to_numeric(
            paid[column] if column in paid.columns else pd.Series(index=paid.index, dtype=float),
            errors="coerce",
        ).fillna(0.0)
    paid["valor_comissao_total"] = pd.to_numeric(
        paid["valor_comissao_total"] if "valor_comissao_total" in paid.columns else pd.Series(index=paid.index, dtype=float),
        errors="coerce",
    )
    paid["valor_comissao_total"] = paid["valor_comissao_total"].fillna(
        paid["valor_comissao_fat"] + paid["valor_comissao_margem"]
    )
    return paid


def _aggregate_cen_commission_columns(frame: pd.DataFrame) -> pd.Series:
    return pd.Series(
        {
            "Comissão Faturamento": frame.loc[
                frame["__classificacao"].eq("MAQUINAS JD - NOVOS"),
                "valor_comissao_fat",
            ].sum(),
            "Venda Direta": frame.loc[
                frame["__classificacao"].eq("VENDA DIRETA"),
                "valor_comissao_fat",
            ].sum(),
            "Comissão Usados": frame.loc[
                frame["__classificacao"].eq("MAQUINAS JD - USADOS"),
                "valor_comissao_fat",
            ].sum(),
            "Comissão Implementos": frame.loc[
                frame["__classificacao"].eq("IMPLEMENTO"),
                "valor_comissao_fat",
            ].sum(),
            "Comissão Invasão de área": frame.loc[
                frame["__classificacao"].eq("INVASAO DE AREA"),
                "valor_comissao_fat",
            ].sum(),
            "Comissão Margem": frame["valor_comissao_margem"].sum(),
        }
    )


def _build_analytic_report_df(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=[label for _, label in ANALYTIC_REPORT_COLUMN_MAP])

    analytic = frame.copy()
    analytic = analytic.rename(columns={source: target for source, target in ANALYTIC_REPORT_COLUMN_MAP})
    ordered_columns = [label for _, label in ANALYTIC_REPORT_COLUMN_MAP]
    for column in ordered_columns:
        if column not in analytic.columns:
            analytic[column] = None
    return analytic[ordered_columns].reset_index(drop=True)


def _sort_by_available(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    available = [column for column in columns if column in frame.columns]
    if not available:
        return frame
    return frame.sort_values(available, na_position="last")


def build_cen_report(
    paid_commissions: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
    period_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    report_columns = [
        "CEN",
        "Comissão Faturamento",
        "Venda Direta",
        "Comissão Usados",
        "Comissão Implementos",
        "Comissão Invasão de área",
        "Comissão Margem",
        "Valor Comissão Total",
    ]
    pending_columns = [
        "Pendência",
        "Cod Vendedor",
        "CEN",
        "Nro Documento",
        "Nro Chassi",
        "Classificação Venda",
        "Valor Comissão Fat.",
        "Valor Comissão Margem",
        "Valor Comissão Total",
    ]

    relations = _prepare_manager_relations(manager_relations)
    if relations.empty:
        return pd.DataFrame(columns=report_columns), pd.DataFrame(columns=pending_columns)

    relations = relations[relations["__vendedor"].ne(relations["__gerente"])].copy()
    relations = relations.drop_duplicates(subset=["__cod_vendedor"], keep="first")
    report_base = relations[["__cod_vendedor", "__vendedor_display"]].rename(columns={"__vendedor_display": "CEN"}).copy()

    paid = _prepare_paid_commissions_for_period(paid_commissions, period_label)
    if paid.empty:
        report = report_base.copy().sort_values("CEN", na_position="last").reset_index(drop=True)
        for column in report_columns[1:]:
            report[column] = 0.0
        return report[report_columns], pd.DataFrame(columns=pending_columns)

    valid_codes = set(report_base["__cod_vendedor"].tolist())
    pending_mask = paid["__cod_vendedor"].eq("") | ~paid["__cod_vendedor"].isin(valid_codes)
    pending = paid.loc[pending_mask].copy()
    pending["Pendência"] = pending["__cod_vendedor"].apply(
        lambda value: "Sem cod_vendedor" if value == "" else "Cod_vendedor sem relação CEN x Gerente"
    )
    if not pending.empty:
        pending = pending.rename(
            columns={
                "cod_vendedor": "Cod Vendedor",
                "cen": "CEN",
                "nro_documento": "Nro Documento",
                "nro_chassi": "Nro Chassi",
                "classificacao_venda": "Classificação Venda",
                "valor_comissao_fat": "Valor Comissão Fat.",
                "valor_comissao_margem": "Valor Comissão Margem",
                "valor_comissao_total": "Valor Comissão Total",
            }
        )
        for column in pending_columns:
            if column not in pending.columns:
                pending[column] = None
        pending = pending[pending_columns].reset_index(drop=True)
    else:
        pending = pd.DataFrame(columns=pending_columns)

    paid = paid.loc[~pending_mask].copy()
    if paid.empty:
        report = report_base.copy().sort_values("CEN", na_position="last").reset_index(drop=True)
        for column in report_columns[1:]:
            report[column] = 0.0
        return report[report_columns], pending

    aggregated = paid.groupby("__cod_vendedor", dropna=False).apply(_aggregate_cen_commission_columns).reset_index()
    aggregated["Valor Comissão Total"] = (
        aggregated["Comissão Faturamento"]
        + aggregated["Venda Direta"]
        + aggregated["Comissão Usados"]
        + aggregated["Comissão Implementos"]
        + aggregated["Comissão Invasão de área"]
        + aggregated["Comissão Margem"]
    )

    report = report_base.merge(aggregated, on="__cod_vendedor", how="left")
    for column in report_columns[1:]:
        report[column] = pd.to_numeric(report.get(column), errors="coerce").fillna(0.0)
    report = report.drop(columns=["__cod_vendedor"]).sort_values("CEN", na_position="last").reset_index(drop=True)
    return report[report_columns], pending


def build_manager_report(
    paid_commissions: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
    period_label: str,
) -> pd.DataFrame:
    report_columns = [
        "Gerente",
        "Receita Bruta",
        "Margem + Incentivos R$",
        "% Margem Bruta",
        "Comissão Total CEN",
        "Comissão Gerente",
    ]

    paid = _prepare_paid_commissions_for_period(paid_commissions, period_label)
    if paid.empty:
        return pd.DataFrame(columns=report_columns)

    paid["__gerente"] = paid.get("gerente", pd.Series(dtype=object)).apply(_normalize_text)
    paid["__gerente_display"] = paid.get("gerente", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    paid = paid[paid["__gerente"].ne("")].copy()
    if paid.empty:
        return pd.DataFrame(columns=report_columns)

    rows: list[dict[str, float | str]] = []
    for manager_key, manager_paid in paid.groupby("__gerente", dropna=False):
        gerente_name = (
            manager_paid["__gerente_display"]
            .loc[manager_paid["__gerente_display"].ne("")]
            .iloc[0]
            if (manager_paid["__gerente_display"].ne("")).any()
            else str(manager_key).strip()
        )

        revenue = manager_paid["receita_bruta"].sum()
        margin_incentives = manager_paid["margem_incentivos_rs"].sum()
        weighted_base = manager_paid["receita_bruta_incentivos_rs"].sum()
        if weighted_base:
            weighted_margin = (
                (manager_paid["perc_margem_bruta"] * manager_paid["receita_bruta_incentivos_rs"]).sum() / weighted_base
            )
        else:
            weighted_margin = 0.0

        commission_total_cen = manager_paid["valor_comissao_total"].sum()

        rows.append(
            {
                "Gerente": gerente_name,
                "Receita Bruta": revenue,
                "Margem + Incentivos R$": margin_incentives,
                "% Margem Bruta": weighted_margin,
                "Comissão Total CEN": commission_total_cen,
                "Comissão Gerente": commission_total_cen * 0.33,
            }
        )

    return pd.DataFrame(rows, columns=report_columns).sort_values("Gerente", na_position="last").reset_index(drop=True)


def build_used_implements_coordinator_report(
    paid_commissions: pd.DataFrame | None,
    period_label: str,
) -> pd.DataFrame:
    report_columns = [
        "Nome",
        "Tipo",
        "Receita Bruta",
        "% Comissão Fat.",
        "Valor Comissão Fat.",
        "Margem + Incentivos R$",
        "Meta Margem",
        "% MB Realizado",
        "% Comissão Margem",
        "Valor Comissão Margem",
        "Valor Total da Comissão",
    ]

    paid = _prepare_paid_commissions_for_period(paid_commissions, period_label)
    if paid.empty:
        return pd.DataFrame(columns=report_columns)

    type_map = {
        "IMPLEMENTO": "Implemento",
        "MAQUINAS JD - USADOS": "Maquinas JD - Usados",
    }
    filtered = paid.loc[paid["__classificacao"].isin(type_map.keys())].copy()
    if filtered.empty:
        return pd.DataFrame(columns=report_columns)

    rows: list[dict[str, float | str]] = []
    for classification_key, type_label in type_map.items():
        group_df = filtered.loc[filtered["__classificacao"].eq(classification_key)].copy()
        if group_df.empty:
            continue

        receita_bruta = group_df["receita_bruta"].sum()
        margem_incentivos = group_df["margem_incentivos_rs"].sum()
        weighted_base = group_df["receita_bruta_incentivos_rs"].sum()
        if weighted_base:
            mb_realizado = (
                (group_df["perc_margem_bruta"] * group_df["receita_bruta_incentivos_rs"]).sum() / weighted_base
            )
        else:
            mb_realizado = 0.0

        perc_comissao_fat = 0.2
        perc_comissao_margem = 0.2
        meta_margem = 15.0
        valor_comissao_fat = receita_bruta * (perc_comissao_fat / 100)
        valor_comissao_margem = receita_bruta * (perc_comissao_margem / 100)

        rows.append(
            {
                "Nome": "Wagner Goncalves Garcia",
                "Tipo": type_label,
                "Receita Bruta": receita_bruta,
                "% Comissão Fat.": perc_comissao_fat,
                "Valor Comissão Fat.": valor_comissao_fat,
                "Margem + Incentivos R$": margem_incentivos,
                "Meta Margem": meta_margem,
                "% MB Realizado": mb_realizado,
                "% Comissão Margem": perc_comissao_margem,
                "Valor Comissão Margem": valor_comissao_margem,
                "Valor Total da Comissão": valor_comissao_fat + valor_comissao_margem,
            }
        )

    return pd.DataFrame(rows, columns=report_columns)


def build_filial_analytic_reports(
    paid_commissions: pd.DataFrame | None,
    period_label: str,
) -> list[tuple[str, pd.DataFrame]]:
    paid = _prepare_paid_commissions_for_period(paid_commissions, period_label)
    if paid.empty:
        return []

    paid = paid.loc[paid["valor_comissao_total"].ne(0)].copy()
    if paid.empty:
        return []

    reports: list[tuple[str, pd.DataFrame]] = []
    filiais = sorted([filial for filial in paid["__filial"].dropna().unique().tolist() if str(filial).strip()])
    for filial in filiais:
        filial_df = paid.loc[paid["__filial"].eq(filial)].copy()
        if filial_df.empty:
            continue
        filial_df = _sort_by_available(filial_df, ["data_emissao", "nro_documento", "nro_chassi"])
        reports.append((str(filial).strip(), _build_analytic_report_df(filial_df)))
    return reports


def build_used_implements_analytic_report(
    paid_commissions: pd.DataFrame | None,
    period_label: str,
) -> pd.DataFrame:
    paid = _prepare_paid_commissions_for_period(paid_commissions, period_label)
    if paid.empty:
        return _build_analytic_report_df(pd.DataFrame())

    filtered = paid.loc[paid["__classificacao"].isin(["IMPLEMENTO", "MAQUINAS JD - USADOS"])].copy()
    if filtered.empty:
        return _build_analytic_report_df(pd.DataFrame())

    filtered = _sort_by_available(
        filtered,
        ["classificacao_venda", "filial", "data_emissao", "nro_documento", "nro_chassi"],
    )
    return _build_analytic_report_df(filtered)
