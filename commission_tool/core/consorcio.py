"""Business rules for the consortium commission flow."""

from __future__ import annotations

import unicodedata

import pandas as pd

from commission_tool.core.formatting import normalize_pdv


STATUS_APTO = "✅ APTO"
STATUS_SEM_COMISSAO = "Sem comissão"
STATUS_BLOQUEADO_PDV = "❌ Bloqueado: PDV sem de-para"
STATUS_BLOQUEADO_VENDEDOR = "❌ Bloqueado: vendedor sem de-para"
STATUS_BLOQUEADO_GERENTE = "❌ Bloqueado: sem gerente"
STATUS_BLOQUEADO_JA_PAGO = "❌ Bloqueado: já pago em {competencia}"
STATUS_BLOQUEADO_DUPLICADO = "❌ Bloqueado: duplicado no upload"
STATUS_BLOQUEADO_REGRA = "❌ Bloqueado: parcela sem regra"
STATUS_BLOQUEADO_VALOR = "❌ Bloqueado: valor do bem inválido"


def normalize_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = unicodedata.normalize("NFKD", str(value).strip())
    return "".join(char for char in text if not unicodedata.combining(char)).upper()


def normalize_code(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().upper()


def normalize_parcela(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return " ".join(str(value).split()).upper()


def cota_key(df: pd.DataFrame) -> pd.Series:
    """Unique key of a cota installment: grupo || cota || versao || parcela."""
    return (
        df["cd_grupo"].apply(normalize_code)
        + "||"
        + df["cd_cota"].apply(normalize_code)
        + "||"
        + df["versao"].apply(normalize_code)
        + "||"
        + df["parcela"].apply(normalize_parcela)
    )


def enrich_consorcio_rows(
    df: pd.DataFrame,
    pdv_map: pd.DataFrame | None,
    vendedor_map: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
    regras: pd.DataFrame | None,
    coordenador: dict | None,
    paid_keys: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Resolve de-paras, apply the rules and classify every row with a status."""
    result = df.copy().reset_index(drop=True)
    if result.empty:
        for column in [
            "filial",
            "cod_vendedor",
            "cen",
            "gerente",
            "coordenador",
            "percentual_cen",
            "valor_comissao_cen",
            "percentual_gestor",
            "valor_comissao_gestor",
            "percentual_coordenador",
            "valor_comissao_coordenador",
            "status",
        ]:
            result[column] = pd.Series(dtype=object)
        result["apto"] = pd.Series(dtype=bool)
        return result

    coordenador = coordenador or {}
    result["__pdv_key"] = result["cd_pdv"].apply(normalize_pdv)
    result["__vendedor_key"] = result["cd_vendedor_consorcio"].apply(normalize_code)
    result["__parcela"] = result["parcela"].apply(normalize_parcela)
    result["__cota_key"] = cota_key(result)

    pdv_lookup = {}
    if pdv_map is not None and not pdv_map.empty:
        pdv_lookup = dict(
            zip(pdv_map["cd_pdv"].apply(normalize_pdv), pdv_map["filial"].fillna("").astype(str).str.strip())
        )
    result["filial"] = result["__pdv_key"].map(pdv_lookup).fillna("")

    vendedor_lookup = {}
    if vendedor_map is not None and not vendedor_map.empty:
        vendedor_lookup = dict(
            zip(
                vendedor_map["cd_vendedor_consorcio"].apply(normalize_code),
                vendedor_map["cod_vendedor"].fillna("").astype(str).str.strip().str.upper(),
            )
        )
    result["cod_vendedor"] = result["__vendedor_key"].map(vendedor_lookup).fillna("")

    cen_lookup: dict[str, str] = {}
    gerente_lookup: dict[str, str] = {}
    if manager_relations is not None and not manager_relations.empty:
        relations = manager_relations.copy()
        relations["__cod"] = relations["cod_vendedor"].apply(normalize_code)
        relations = relations[relations["__cod"].ne("")].drop_duplicates(subset=["__cod"], keep="first")
        cen_lookup = dict(zip(relations["__cod"], relations["vendedor"].fillna("").astype(str).str.strip()))
        gerente_lookup = dict(zip(relations["__cod"], relations["gerente"].fillna("").astype(str).str.strip()))
    result["cen"] = result["cod_vendedor"].map(cen_lookup).fillna("")
    result["gerente"] = result["cod_vendedor"].map(gerente_lookup).fillna("")
    result["coordenador"] = str(coordenador.get("nome", "") or "")

    regras_lookup = {}
    if regras is not None and not regras.empty:
        prepared_regras = regras.copy()
        prepared_regras["__parcela"] = prepared_regras["parcela"].apply(normalize_parcela)
        prepared_regras = prepared_regras.drop_duplicates(subset=["__parcela"], keep="first")
        for _, regra in prepared_regras.iterrows():
            regras_lookup[regra["__parcela"]] = (
                float(regra["percentual_cen"] or 0),
                float(regra["percentual_gestor"] or 0),
                float(regra["percentual_coordenador"] or 0),
            )

    result["__tem_regra"] = result["__parcela"].isin(regras_lookup)
    result["percentual_cen"] = result["__parcela"].map(lambda p: regras_lookup.get(p, (0.0, 0.0, 0.0))[0])
    result["percentual_gestor"] = result["__parcela"].map(lambda p: regras_lookup.get(p, (0.0, 0.0, 0.0))[1])
    result["percentual_coordenador"] = result["__parcela"].map(
        lambda p: regras_lookup.get(p, (0.0, 0.0, 0.0))[2]
    )

    vl_bem = pd.to_numeric(result["vl_bem"], errors="coerce")
    result["__valor_invalido"] = vl_bem.isna() | vl_bem.le(0)
    base = vl_bem.fillna(0.0)
    result["valor_comissao_cen"] = base * result["percentual_cen"] / 100
    result["valor_comissao_gestor"] = base * result["percentual_gestor"] / 100
    result["valor_comissao_coordenador"] = base * result["percentual_coordenador"] / 100

    result["__paga_alguem"] = (
        result["percentual_cen"].gt(0)
        | result["percentual_gestor"].gt(0)
        | result["percentual_coordenador"].gt(0)
    )
    result["__duplicado"] = result.duplicated(subset=["__cota_key"], keep="first")
    paid_keys = paid_keys or {}
    result["__ja_pago"] = result["__cota_key"].map(paid_keys)

    def _status(row: pd.Series) -> str:
        if row["__valor_invalido"]:
            return STATUS_BLOQUEADO_VALOR
        if not row["__tem_regra"]:
            return STATUS_BLOQUEADO_REGRA
        if isinstance(row["__ja_pago"], str) and row["__ja_pago"]:
            return STATUS_BLOQUEADO_JA_PAGO.format(competencia=row["__ja_pago"])
        if row["__duplicado"]:
            return STATUS_BLOQUEADO_DUPLICADO
        if not row["filial"]:
            return STATUS_BLOQUEADO_PDV
        if not row["cod_vendedor"]:
            return STATUS_BLOQUEADO_VENDEDOR
        if row["__paga_alguem"] and not row["gerente"]:
            return STATUS_BLOQUEADO_GERENTE
        if not row["__paga_alguem"]:
            return STATUS_SEM_COMISSAO
        return STATUS_APTO

    result["status"] = result.apply(_status, axis=1)
    result["apto"] = result["status"].eq(STATUS_APTO)
    zerar = ~result["apto"]
    for column in ["valor_comissao_cen", "valor_comissao_gestor", "valor_comissao_coordenador"]:
        result.loc[zerar, column] = 0.0

    return result.drop(
        columns=[
            "__pdv_key",
            "__vendedor_key",
            "__parcela",
            "__tem_regra",
            "__valor_invalido",
            "__paga_alguem",
            "__duplicado",
            "__ja_pago",
        ]
    )


def build_premiacao_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the three payout summaries: CEN, store manager and coordinator."""
    empty = {
        "cen": pd.DataFrame(columns=["CEN", "Filial", "Valor a pagar"]),
        "gestor": pd.DataFrame(columns=["Gestor", "Valor a pagar"]),
        "coordenador": pd.DataFrame(columns=["Coordenador", "Valor a pagar"]),
    }
    if df is None or df.empty:
        return empty

    paid = df[df.get("apto", pd.Series([False] * len(df), index=df.index))].copy()
    if paid.empty:
        return empty

    cen = (
        paid.groupby(["cen", "filial"], dropna=False)["valor_comissao_cen"]
        .sum()
        .reset_index()
        .rename(columns={"cen": "CEN", "filial": "Filial", "valor_comissao_cen": "Valor a pagar"})
        .sort_values("CEN")
        .reset_index(drop=True)
    )
    gestor = (
        paid[paid["gerente"].ne("")]
        .groupby("gerente", dropna=False)["valor_comissao_gestor"]
        .sum()
        .reset_index()
        .rename(columns={"gerente": "Gestor", "valor_comissao_gestor": "Valor a pagar"})
        .sort_values("Gestor")
        .reset_index(drop=True)
    )
    coordenador = (
        paid.groupby("coordenador", dropna=False)["valor_comissao_coordenador"]
        .sum()
        .reset_index()
        .rename(columns={"coordenador": "Coordenador", "valor_comissao_coordenador": "Valor a pagar"})
        .reset_index(drop=True)
    )
    return {"cen": cen, "gestor": gestor, "coordenador": coordenador}


CONSORCIO_ANALYTIC_COLUMN_MAP = [
    ("filial", "Filial"),
    ("dt_aprovacao", "Data de Aprovação"),
    ("cd_pedido", "Pedido"),
    ("cd_grupo", "Grupo"),
    ("cd_cota", "Cota"),
    ("versao", "Versão"),
    ("consorciado", "Consorciado"),
    ("parcela", "Parcela"),
    ("vl_bem", "Valor do Bem"),
    ("percentual_cen", "% Comissão CEN"),
    ("valor_comissao_cen", "Valor Comissão CEN"),
    ("cen", "CEN"),
    ("gerente", "Gerente"),
]


def build_consorcio_analytic_df(df: pd.DataFrame | None) -> pd.DataFrame:
    """Build the human-readable consortium analytic report used for e-mail attachments.

    Renames the raw ``consorcio_pago`` columns to their Portuguese labels, in the
    order defined by ``CONSORCIO_ANALYTIC_COLUMN_MAP``, and drops every internal
    database column (id, competência, chaves de vendedor/de-para etc.). A missing
    source column becomes an empty column instead of raising.
    """
    ordered_columns = [label for _, label in CONSORCIO_ANALYTIC_COLUMN_MAP]
    if df is None or df.empty:
        return pd.DataFrame(columns=ordered_columns)

    analytic = df.copy()
    analytic = analytic.rename(columns={source: target for source, target in CONSORCIO_ANALYTIC_COLUMN_MAP})
    for column in ordered_columns:
        if column not in analytic.columns:
            analytic[column] = None
    if "Data de Aprovação" in analytic.columns:
        parsed_dates = pd.to_datetime(analytic["Data de Aprovação"], errors="coerce")
        analytic["Data de Aprovação"] = parsed_dates.dt.strftime("%d/%m/%Y").fillna(
            analytic["Data de Aprovação"]
        )
    return analytic[ordered_columns].reset_index(drop=True)


def suggest_cod_vendedor(
    vendedor_map: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
) -> pd.DataFrame:
    """Suggest an ERP seller code by exact name match. Never fills cod_vendedor."""
    columns = ["cd_vendedor_consorcio", "nm_vendedor_consorcio", "cod_vendedor", "sugestao_cod_vendedor", "sugestao_cen"]
    if vendedor_map is None or vendedor_map.empty:
        return pd.DataFrame(columns=columns)

    result = vendedor_map.copy().reset_index(drop=True)
    result["cod_vendedor"] = result["cod_vendedor"].fillna("").astype(str).str.strip().str.upper()

    by_name: dict[str, tuple[str, str]] = {}
    if manager_relations is not None and not manager_relations.empty:
        relations = manager_relations.copy()
        relations["__nome"] = relations["vendedor"].apply(normalize_text)
        relations = relations[relations["__nome"].ne("")].drop_duplicates(subset=["__nome"], keep="first")
        by_name = {
            row["__nome"]: (normalize_code(row["cod_vendedor"]), str(row["vendedor"]).strip())
            for _, row in relations.iterrows()
        }

    nomes = result["nm_vendedor_consorcio"].apply(normalize_text)
    result["sugestao_cod_vendedor"] = nomes.map(lambda nome: by_name.get(nome, ("", ""))[0])
    result["sugestao_cen"] = nomes.map(lambda nome: by_name.get(nome, ("", ""))[1])
    return result[columns]
