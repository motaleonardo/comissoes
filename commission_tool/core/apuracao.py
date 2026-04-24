"""Business rules for the machine commission apuracao flow."""

from __future__ import annotations

from typing import Any

import pandas as pd

from commission_tool.core.formatting import (
    parse_br_number,
    parse_commission_percent_points,
    parse_margin_target_percent_points,
    parse_percent_points,
)


IMPLEMENTO_THRESHOLD = 200000.0
IMPLEMENTO_RATE_AT_OR_ABOVE_THRESHOLD = 0.84
IMPLEMENTO_RATE_BELOW_THRESHOLD = 1.0
USED_MACHINE_RATE = 1.0


def normalize_model(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def normalize_chassi(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def normalize_seller_code(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _find_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
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


def _prepare_rate_table(
    df: pd.DataFrame | None,
    *,
    include_meta: bool = False,
) -> pd.DataFrame:
    if df is None or df.empty:
        columns = ["modelo", "percentual"]
        if include_meta:
            columns.append("meta_margem")
        return pd.DataFrame(columns=columns)

    modelo_col = _find_column(df, ["modelo", "Modelo", "MODELO"])
    percent_col = _find_column(
        df,
        [
            "percentual",
            "Percentual",
            "% Comissão Fat.",
            "% Comissão Fat",
            "% Comissao Fat.",
            "% Comissao Fat",
            "% Comissão Margem",
            "% Comissão Margem.",
            "% Comissao Margem",
            "% Comissao Margem.",
        ],
    )
    meta_col = _find_column(
        df,
        [
            "Meta",
            "meta_margem",
            "Meta de Margem",
            "Meta Margem",
            "% Meta de Margem",
            "% Meta Margem",
            "Meta de Margem %",
            "Meta Margem %",
        ],
        required=False,
    )

    prepared = pd.DataFrame(
        {
            "modelo": df[modelo_col].apply(normalize_model),
            "percentual": df[percent_col].apply(parse_commission_percent_points),
        }
    )
    if include_meta:
        if meta_col:
            prepared["meta_margem"] = df[meta_col].apply(parse_margin_target_percent_points)
        else:
            prepared["meta_margem"] = 0.0

    prepared = prepared[prepared["modelo"].ne("")]
    return prepared.drop_duplicates(subset=["modelo"], keep="first").reset_index(drop=True)


def apply_commission_rules(
    df_machine: pd.DataFrame,
    fat_rates: pd.DataFrame | None,
    margin_rates: pd.DataFrame | None,
) -> pd.DataFrame:
    """Apply model-based Postgres rules and recalculate commission values."""
    result = df_machine.copy()
    if result.empty:
        return result

    fat_table = _prepare_rate_table(fat_rates)
    margin_table = _prepare_rate_table(margin_rates, include_meta=True)

    fat_lookup = dict(zip(fat_table["modelo"], fat_table["percentual"]))
    margin_lookup = dict(zip(margin_table["modelo"], margin_table["percentual"]))
    meta_lookup = dict(zip(margin_table["modelo"], margin_table.get("meta_margem", pd.Series(dtype=float))))

    normalized_model = result.get("Modelo", pd.Series([""] * len(result), index=result.index)).apply(normalize_model)
    normalized_classification = (
        result.get("Classificação Venda", pd.Series([""] * len(result), index=result.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    receita_bruta = result.get("Receita Bruta", pd.Series([0.0] * len(result), index=result.index)).apply(parse_br_number)
    receita_bruta = pd.to_numeric(receita_bruta, errors="coerce").fillna(0.0)
    margem_bruta_available = "% Margem Bruta" in result.columns
    margem_bruta = result.get("% Margem Bruta", pd.Series([0.0] * len(result), index=result.index)).apply(
        parse_margin_target_percent_points
    )
    margem_bruta = pd.to_numeric(margem_bruta, errors="coerce").fillna(0.0)

    result["% Comissão Fat."] = normalized_model.map(fat_lookup).fillna(0.0).astype(float)
    result["Meta de Margem"] = normalized_model.map(meta_lookup).fillna(0.0).astype(float)
    result["% Comissão Margem"] = normalized_model.map(margin_lookup).fillna(0.0).astype(float)
    result["Regra Comissão Fat. Encontrada"] = normalized_model.isin(fat_lookup)
    result["Regra Comissão Margem Encontrada"] = normalized_model.isin(margin_lookup)
    result["Regra Classificação Aplicada"] = False

    implemento_mask = normalized_classification.eq("IMPLEMENTO")
    used_machine_mask = normalized_classification.eq("MAQUINAS JD - USADOS")
    classification_override_mask = implemento_mask | used_machine_mask

    result.loc[implemento_mask & receita_bruta.ge(IMPLEMENTO_THRESHOLD), "% Comissão Fat."] = (
        IMPLEMENTO_RATE_AT_OR_ABOVE_THRESHOLD
    )
    result.loc[implemento_mask & ~receita_bruta.ge(IMPLEMENTO_THRESHOLD), "% Comissão Fat."] = (
        IMPLEMENTO_RATE_BELOW_THRESHOLD
    )
    result.loc[used_machine_mask, "% Comissão Fat."] = USED_MACHINE_RATE
    result.loc[classification_override_mask, ["% Comissão Margem", "Meta de Margem"]] = 0.0
    result.loc[classification_override_mask, "Regra Comissão Fat. Encontrada"] = True
    result.loc[classification_override_mask, "Regra Comissão Margem Encontrada"] = True
    result.loc[classification_override_mask, "Regra Classificação Aplicada"] = True

    result["Gatilho Comissão Margem Atingido"] = (
        ((margem_bruta >= result["Meta de Margem"]) & result["% Comissão Margem"].gt(0))
        if margem_bruta_available
        else False
    )
    result.loc[classification_override_mask, "Gatilho Comissão Margem Atingido"] = False
    result["Valor Comissão Fat."] = receita_bruta * result["% Comissão Fat."] / 100
    result["Valor Comissão Margem"] = (
        receita_bruta
        * result["% Comissão Margem"]
        / 100
        * result["Gatilho Comissão Margem Atingido"].astype(float)
    )
    result["Valor Comissão Total"] = result["Valor Comissão Fat."] + result["Valor Comissão Margem"]

    return result


def apply_manager_commission_rules(
    df_machine: pd.DataFrame,
    manager_relations: pd.DataFrame | None,
) -> pd.DataFrame:
    result = df_machine.copy()
    if result.empty:
        return result

    manager_percent_column = "% Comissão Gerente"
    manager_value_column = "Valor Comissão Gerente"
    total_commission_col = _find_column(
        result,
        ["Valor Comissão Total", "Valor ComissÃ£o Total", "valor_comissao_total"],
        required=False,
    )

    result["Cod Vendedor"] = result.get(
        "Cod Vendedor",
        pd.Series([""] * len(result), index=result.index),
    ).apply(normalize_seller_code)

    if manager_relations is None or manager_relations.empty:
        result["Gerente"] = result.get("Gerente", "")
        result[manager_percent_column] = 0.0
        result[manager_value_column] = 0.0
        result["Regra Gerente Encontrada"] = False
        return result

    seller_col = _find_column(
        manager_relations,
        ["cod_vendedor", "Cod Vendedor", "CodVendedor", "CÃ³digo Vendedor", "Codigo Vendedor"],
    )
    gerente_col = _find_column(manager_relations, ["gerente", "Gerente"])
    pct_col = _find_column(
        manager_relations,
        [
            "percentual_comissao_gerente",
            "% ComissÃ£o Gerente",
            "% Comissao Gerente",
            "Percentual ComissÃ£o Gerente",
            "Percentual Comissao Gerente",
        ],
        required=False,
    )

    prepared = pd.DataFrame(
        {
            "Cod Vendedor": manager_relations[seller_col].apply(normalize_seller_code),
            "Gerente": manager_relations[gerente_col].fillna("").astype(str).str.strip(),
            manager_percent_column: (
                manager_relations[pct_col].apply(parse_percent_points)
                if pct_col
                else 33.0
            ),
        }
    )
    prepared = prepared[prepared["Cod Vendedor"].ne("")].drop_duplicates(subset=["Cod Vendedor"], keep="first")
    result = result.merge(prepared, on="Cod Vendedor", how="left", suffixes=("", "_manager"))
    result["Gerente"] = result["Gerente"].fillna("").astype(str).str.strip()
    result[manager_percent_column] = pd.to_numeric(result[manager_percent_column], errors="coerce").fillna(0.0)
    total_commission = (
        pd.to_numeric(result[total_commission_col], errors="coerce").fillna(0.0)
        if total_commission_col
        else pd.Series([0.0] * len(result), index=result.index, dtype=float)
    )
    result[manager_value_column] = (
        total_commission
        * result[manager_percent_column]
        / 100
    )
    result["Regra Gerente Encontrada"] = result["Gerente"].ne("")
    return result


def apply_frontend_default_fat_commission(
    df_machine: pd.DataFrame,
    enabled: bool,
) -> pd.DataFrame:
    """Frontend-only override for apurações: default fat commission to 1% when zero."""
    result = df_machine.copy()
    if result.empty:
        result["Padrão 1% Fat. Aplicado"] = pd.Series(dtype=bool)
        return result

    pct_col = _find_column(
        result,
        ["% Comissão Fat.", "% ComissÃ£o Fat.", "perc_comissao_fat"],
        required=False,
    )
    receita_col = _find_column(result, ["Receita Bruta", "receita_bruta"], required=False)
    valor_fat_col = _find_column(
        result,
        ["Valor Comissão Fat.", "Valor ComissÃ£o Fat.", "valor_comissao_fat"],
        required=False,
    )
    valor_margem_col = _find_column(
        result,
        ["Valor Comissão Margem", "Valor ComissÃ£o Margem", "valor_comissao_margem"],
        required=False,
    )
    valor_total_col = _find_column(
        result,
        ["Valor Comissão Total", "Valor ComissÃ£o Total", "valor_comissao_total"],
        required=False,
    )

    if not all([pct_col, receita_col, valor_fat_col, valor_total_col]):
        result["Padrão 1% Fat. Aplicado"] = False
        return result

    pct_values = pd.to_numeric(result[pct_col], errors="coerce").fillna(0.0)
    receita_values = pd.to_numeric(result[receita_col], errors="coerce").fillna(0.0)
    margem_values = (
        pd.to_numeric(result[valor_margem_col], errors="coerce").fillna(0.0)
        if valor_margem_col
        else pd.Series([0.0] * len(result), index=result.index, dtype=float)
    )

    default_mask = enabled & pct_values.eq(0.0)
    if enabled:
        pct_values = pct_values.mask(default_mask, 1.0)

    result[pct_col] = pct_values
    result[valor_fat_col] = receita_values * pct_values / 100
    result[valor_total_col] = result[valor_fat_col] + margem_values
    result["Padrão 1% Fat. Aplicado"] = default_mask.astype(bool)
    return result


def _paid_status(row: pd.Series) -> str:
    qtd = int(row.get("Qtd Lançamentos Pagos", 0) or 0)
    saldo = float(row.get("Saldo Comissão Paga Chassi", 0) or 0)
    tem_positivo = bool(row.get("Tem Pagamento Positivo", False))
    tem_negativo = bool(row.get("Tem Estorno Negativo", False))

    if qtd == 0 and not tem_positivo and not tem_negativo:
        return "Novo para apuração"
    if tem_positivo and not tem_negativo and saldo > 0:
        return "Já pago"
    if tem_negativo and tem_positivo:
        if saldo > 0:
            return "Atenção: pagamento e estorno parcial"
        if saldo < 0:
            return "Atenção: histórico líquido negativo"
        return "Atenção: comissão anterior zerada por estorno"
    if tem_negativo:
        return "Atenção: chassi com devolução/estorno histórico"
    return "Verificar histórico de comissões pagas"


def apply_paid_history_filter(
    df_machine: pd.DataFrame,
    paid_summary: pd.DataFrame | None,
    excluded_summary: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter out clearly paid chassis while keeping negative history for review."""
    result = df_machine.copy()
    result["__chassi_key"] = result.get("Nro Chassi", pd.Series([""] * len(result), index=result.index)).apply(
        normalize_chassi
    )

    if paid_summary is None or paid_summary.empty:
        result["Qtd Lançamentos Pagos"] = 0
        result["Valor Pago Histórico"] = 0.0
        result["Valor Estornado Histórico"] = 0.0
        result["Saldo Comissão Paga Chassi"] = 0.0
        result["Tem Pagamento Positivo"] = False
        result["Tem Estorno Negativo"] = False
    else:
        summary = paid_summary.copy()
        chassi_col = _find_column(summary, ["nro_chassi", "Nro Chassi", "NRO_CHASSI"])
        summary["__chassi_key"] = summary[chassi_col].apply(normalize_chassi)
        rename_map = {
            "qtd_lancamentos_pagos": "Qtd Lançamentos Pagos",
            "valor_pago_positivo": "Valor Pago Histórico",
            "valor_estornado_negativo": "Valor Estornado Histórico",
            "saldo_comissao_paga_chassi": "Saldo Comissão Paga Chassi",
            "tem_pagamento_positivo": "Tem Pagamento Positivo",
            "tem_estorno_negativo": "Tem Estorno Negativo",
        }
        summary = summary.rename(columns=rename_map)
        keep_cols = ["__chassi_key", *rename_map.values()]
        for column in keep_cols:
            if column not in summary.columns:
                summary[column] = 0
        summary = summary[keep_cols].drop_duplicates(subset=["__chassi_key"], keep="first")
        result = result.merge(summary, on="__chassi_key", how="left")

    if excluded_summary is None or excluded_summary.empty:
        result["Qtd Lançamentos Excluídos"] = 0
        result["Bloqueado por Exclusão"] = False
    else:
        excluded = excluded_summary.copy()
        excluded_chassi_col = _find_column(excluded, ["nro_chassi", "Nro Chassi", "NRO_CHASSI"])
        excluded["__chassi_key"] = excluded[excluded_chassi_col].apply(normalize_chassi)
        excluded = excluded.rename(columns={"qtd_lancamentos_excluidos": "Qtd Lançamentos Excluídos"})
        if "Qtd Lançamentos Excluídos" not in excluded.columns:
            excluded["Qtd Lançamentos Excluídos"] = 1
        excluded = excluded[["__chassi_key", "Qtd Lançamentos Excluídos"]].drop_duplicates(
            subset=["__chassi_key"],
            keep="first",
        )
        result = result.merge(excluded, on="__chassi_key", how="left")
        result["Qtd Lançamentos Excluídos"] = pd.to_numeric(
            result["Qtd Lançamentos Excluídos"],
            errors="coerce",
        ).fillna(0)
        result["Bloqueado por Exclusão"] = result["Qtd Lançamentos Excluídos"] > 0

    numeric_columns = [
        "Qtd Lançamentos Pagos",
        "Valor Pago Histórico",
        "Valor Estornado Histórico",
        "Saldo Comissão Paga Chassi",
        "Qtd Lançamentos Excluídos",
    ]
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0)
    for column in ["Tem Pagamento Positivo", "Tem Estorno Negativo"]:
        result[column] = result[column].fillna(False).astype(bool)

    result["Status Confronto Pagas"] = result.apply(_paid_status, axis=1)
    result.loc[result["Bloqueado por Exclusão"], "Status Confronto Pagas"] = "Excluído manualmente"
    result["Bloqueado por Pagamento Histórico"] = (
        result["Tem Pagamento Positivo"]
        & ~result["Tem Estorno Negativo"]
        & (result["Saldo Comissão Paga Chassi"] > 0)
    )

    full_history = result.drop(columns=["__chassi_key"])
    candidates = full_history[
        ~full_history["Bloqueado por Pagamento Histórico"]
        & ~full_history["Bloqueado por Exclusão"]
    ].reset_index(drop=True)
    return candidates, full_history.reset_index(drop=True)


def prepare_machine_apuracao(
    df_machine: pd.DataFrame,
    fat_rates: pd.DataFrame | None,
    margin_rates: pd.DataFrame | None,
    paid_summary: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None = None,
    excluded_summary: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    calculated = apply_commission_rules(df_machine, fat_rates, margin_rates)
    calculated = apply_manager_commission_rules(calculated, manager_relations)
    return apply_paid_history_filter(calculated, paid_summary, excluded_summary)
