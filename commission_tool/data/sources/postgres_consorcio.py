"""Postgres access for the consortium commission flow."""

from __future__ import annotations

import pandas as pd

from commission_tool.core.formatting import normalize_pdv, parse_br_number, parse_commission_percent_points
from commission_tool.data.sources.postgres import get_engine


CONSORCIO_PDV_TABLE = "comissao_consorcio_pdv"
CONSORCIO_VENDEDOR_TABLE = "comissao_consorcio_vendedor"
CONSORCIO_REGRAS_TABLE = "comissao_consorcio_regras"
CONSORCIO_COORDENADOR_TABLE = "comissao_consorcio_coordenador"
CONSORCIO_PAID_TABLE = "comissoes_consorcio_pagas"

DEFAULT_REGRAS = [
    {"parcela": "001 / 003", "percentual_cen": 0.84, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
    {"parcela": "002 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.21, "percentual_coordenador": 0.15},
    {"parcela": "003 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
]
DEFAULT_COORDENADOR = {"nome": "DJALMA RODRIGUES DE PAULA JUNIOR", "email": "djalma.junior@hohl.com.br"}


def ensure_consorcio_tables() -> None:
    """Create the consortium tables when they do not exist and seed the rules."""
    from sqlalchemy import text

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_PDV_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    cd_pdv TEXT NOT NULL,
                    nm_pdv TEXT,
                    filial TEXT NOT NULL,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_VENDEDOR_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    cd_vendedor_consorcio TEXT NOT NULL,
                    nm_vendedor_consorcio TEXT,
                    cod_vendedor TEXT NOT NULL DEFAULT '',
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_REGRAS_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    parcela TEXT NOT NULL,
                    percentual_cen NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    percentual_gestor NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    percentual_coordenador NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_COORDENADOR_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    nome TEXT NOT NULL,
                    email TEXT,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_PAID_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    paid_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    competencia_ano INTEGER NOT NULL,
                    competencia_mes INTEGER NOT NULL,
                    mes_ano_comissao TEXT NOT NULL,
                    arquivo_origem TEXT,
                    cd_pedido TEXT,
                    cd_grupo TEXT NOT NULL,
                    cd_cota TEXT NOT NULL,
                    versao TEXT NOT NULL,
                    parcela TEXT NOT NULL,
                    st_contemplacao TEXT,
                    dt_alocacao DATE,
                    dt_aprovacao DATE,
                    consorciado TEXT,
                    vl_bem NUMERIC(18, 2),
                    cd_pdv TEXT,
                    filial TEXT,
                    cd_vendedor_consorcio TEXT,
                    nm_vendedor_consorcio TEXT,
                    cod_vendedor TEXT,
                    cen TEXT,
                    gerente TEXT,
                    coordenador TEXT,
                    percentual_cen NUMERIC(12, 6),
                    valor_comissao_cen NUMERIC(18, 2),
                    percentual_gestor NUMERIC(12, 6),
                    valor_comissao_gestor NUMERIC(18, 2),
                    percentual_coordenador NUMERIC(12, 6),
                    valor_comissao_coordenador NUMERIC(18, 2)
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {CONSORCIO_PAID_TABLE}_cota_parcela_uidx
                ON {CONSORCIO_PAID_TABLE} (cd_grupo, cd_cota, versao, parcela)
                """
            )
        )
        seeded = conn.execute(text(f"SELECT COUNT(*) FROM {CONSORCIO_REGRAS_TABLE}")).scalar()
        if not seeded:
            pd.DataFrame(DEFAULT_REGRAS).assign(ativo=True).to_sql(
                CONSORCIO_REGRAS_TABLE, conn, if_exists="append", index=False
            )
        seeded_coord = conn.execute(text(f"SELECT COUNT(*) FROM {CONSORCIO_COORDENADOR_TABLE}")).scalar()
        if not seeded_coord:
            pd.DataFrame([DEFAULT_COORDENADOR]).assign(ativo=True).to_sql(
                CONSORCIO_COORDENADOR_TABLE, conn, if_exists="append", index=False
            )


def _prepare_pdv_map(df: pd.DataFrame) -> pd.DataFrame:
    source = df.rename(
        columns={
            "CD_Comissionado_PDV": "cd_pdv",
            "NM_Comissionado_PDV": "nm_pdv",
            "Filial": "filial",
            "PDV": "cd_pdv",
        }
    )
    prepared = pd.DataFrame(
        {
            "cd_pdv": source.get("cd_pdv", pd.Series(dtype=object)).apply(normalize_pdv),
            "nm_pdv": source.get("nm_pdv", pd.Series(dtype=object)).fillna("").astype(str).str.strip(),
            "filial": source.get("filial", pd.Series(dtype=object)).fillna("").astype(str).str.strip(),
        }
    )
    prepared = prepared[prepared["cd_pdv"].ne("") & prepared["filial"].ne("")]
    prepared = prepared.drop_duplicates(subset=["cd_pdv"], keep="first")
    prepared["ativo"] = True
    return prepared.reset_index(drop=True)


def _prepare_vendedor_map(df: pd.DataFrame) -> pd.DataFrame:
    source = df.rename(
        columns={
            "CD_Comissionado_Vendedor": "cd_vendedor_consorcio",
            "NM_Comissionado_Vendedor": "nm_vendedor_consorcio",
            "Cod Vendedor": "cod_vendedor",
        }
    )
    prepared = pd.DataFrame(
        {
            "cd_vendedor_consorcio": source.get("cd_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
            "nm_vendedor_consorcio": source.get("nm_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
            "cod_vendedor": source.get("cod_vendedor", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper(),
        }
    )
    prepared = prepared[prepared["cd_vendedor_consorcio"].ne("")]
    prepared = prepared.drop_duplicates(subset=["cd_vendedor_consorcio"], keep="first")
    prepared["ativo"] = True
    return prepared.reset_index(drop=True)


def _read_active(table: str, columns: list[str]) -> pd.DataFrame:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    query = text(f"SELECT {', '.join(columns)} FROM {table} WHERE ativo IS TRUE ORDER BY id")
    return pd.read_sql(query, engine)


def _replace_active(table: str, prepared: pd.DataFrame) -> int:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE {table} SET ativo = FALSE WHERE ativo IS TRUE"))
        prepared.to_sql(table, conn, if_exists="append", index=False)
    return len(prepared)


def read_consorcio_pdv_map() -> pd.DataFrame:
    return _read_active(CONSORCIO_PDV_TABLE, ["cd_pdv", "nm_pdv", "filial"])


def replace_active_consorcio_pdv_map(df: pd.DataFrame) -> int:
    return _replace_active(CONSORCIO_PDV_TABLE, _prepare_pdv_map(df))


def read_consorcio_vendedor_map() -> pd.DataFrame:
    return _read_active(
        CONSORCIO_VENDEDOR_TABLE,
        ["cd_vendedor_consorcio", "nm_vendedor_consorcio", "cod_vendedor"],
    )


def replace_active_consorcio_vendedor_map(df: pd.DataFrame) -> int:
    return _replace_active(CONSORCIO_VENDEDOR_TABLE, _prepare_vendedor_map(df))


def register_missing_consorcio_vendedores(df: pd.DataFrame) -> int:
    """Insert unknown consortium sellers with a blank cod_vendedor, for later fill-in."""
    ensure_consorcio_tables()
    known = read_consorcio_vendedor_map()
    known_codes = set(known.get("cd_vendedor_consorcio", pd.Series(dtype=object)).astype(str).str.strip())

    candidates = pd.DataFrame(
        {
            "cd_vendedor_consorcio": df.get("cd_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
            "nm_vendedor_consorcio": df.get("nm_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
        }
    )
    candidates = candidates[candidates["cd_vendedor_consorcio"].ne("")]
    candidates = candidates[~candidates["cd_vendedor_consorcio"].isin(known_codes)]
    candidates = candidates.drop_duplicates(subset=["cd_vendedor_consorcio"], keep="first")
    if candidates.empty:
        return 0

    candidates["cod_vendedor"] = ""
    candidates["ativo"] = True
    engine = get_engine()
    with engine.begin() as conn:
        candidates.to_sql(CONSORCIO_VENDEDOR_TABLE, conn, if_exists="append", index=False)
    return len(candidates)


def read_consorcio_regras() -> pd.DataFrame:
    return _read_active(
        CONSORCIO_REGRAS_TABLE,
        ["parcela", "percentual_cen", "percentual_gestor", "percentual_coordenador"],
    )


def _prepare_consorcio_regras(df: pd.DataFrame) -> pd.DataFrame:
    prepared = pd.DataFrame(
        {
            "parcela": df.get("parcela", pd.Series(dtype=object)).fillna("").astype(str).str.strip(),
            "percentual_cen": df.get("percentual_cen", pd.Series(dtype=object)).apply(
                parse_commission_percent_points
            ),
            "percentual_gestor": df.get("percentual_gestor", pd.Series(dtype=object)).apply(
                parse_commission_percent_points
            ),
            "percentual_coordenador": df.get("percentual_coordenador", pd.Series(dtype=object)).apply(
                parse_commission_percent_points
            ),
        }
    )
    prepared = prepared[prepared["parcela"].ne("")].drop_duplicates(subset=["parcela"], keep="first")
    prepared["ativo"] = True
    return prepared.reset_index(drop=True)


def replace_active_consorcio_regras(df: pd.DataFrame) -> int:
    return _replace_active(CONSORCIO_REGRAS_TABLE, _prepare_consorcio_regras(df))


def read_consorcio_coordenador() -> dict:
    rows = _read_active(CONSORCIO_COORDENADOR_TABLE, ["nome", "email"])
    if rows.empty:
        return dict(DEFAULT_COORDENADOR)
    return {"nome": str(rows.loc[0, "nome"] or ""), "email": str(rows.loc[0, "email"] or "")}


def save_consorcio_coordenador(nome: str, email: str) -> None:
    prepared = pd.DataFrame([{"nome": str(nome).strip(), "email": str(email).strip(), "ativo": True}])
    _replace_active(CONSORCIO_COORDENADOR_TABLE, prepared)


CONSORCIO_PAID_COLUMNS = [
    "arquivo_origem",
    "cd_pedido",
    "cd_grupo",
    "cd_cota",
    "versao",
    "parcela",
    "st_contemplacao",
    "dt_alocacao",
    "dt_aprovacao",
    "consorciado",
    "vl_bem",
    "cd_pdv",
    "filial",
    "cd_vendedor_consorcio",
    "nm_vendedor_consorcio",
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
]


def _prepare_consorcio_paid(
    df: pd.DataFrame,
    competence_year: int,
    competence_month: int,
    period_label: str,
) -> pd.DataFrame:
    source = df.copy()
    apto = source.get("apto", pd.Series([False] * len(source), index=source.index))
    source = source[apto.fillna(False).astype(bool)]

    prepared = pd.DataFrame(index=range(len(source)))
    source = source.reset_index(drop=True)
    for column in CONSORCIO_PAID_COLUMNS:
        prepared[column] = source[column] if column in source.columns else None

    for column in ["dt_alocacao", "dt_aprovacao"]:
        parsed = pd.to_datetime(prepared[column], dayfirst=True, errors="coerce").dt.date
        prepared[column] = parsed.astype(object).where(parsed.notna(), None)
    for column in [
        "vl_bem",
        "percentual_cen",
        "valor_comissao_cen",
        "percentual_gestor",
        "valor_comissao_gestor",
        "percentual_coordenador",
        "valor_comissao_coordenador",
    ]:
        prepared[column] = pd.to_numeric(prepared[column].apply(parse_br_number), errors="coerce")

    prepared.insert(0, "mes_ano_comissao", period_label)
    prepared.insert(0, "competencia_mes", int(competence_month))
    prepared.insert(0, "competencia_ano", int(competence_year))
    return prepared.reset_index(drop=True)


def save_consorcio_paid(
    df: pd.DataFrame,
    competence_year: int,
    competence_month: int,
    period_label: str,
) -> int:
    """Persist the selected consortium rows. The unique index blocks a repeated cota."""
    ensure_consorcio_tables()
    prepared = _prepare_consorcio_paid(df, competence_year, competence_month, period_label)
    if prepared.empty:
        return 0
    engine = get_engine()
    with engine.begin() as conn:
        prepared.to_sql(CONSORCIO_PAID_TABLE, conn, if_exists="append", index=False)
    return len(prepared)


def read_consorcio_paid_keys() -> dict[str, str]:
    """Map every cota installment already paid to the competence it was paid in."""
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    rows = pd.read_sql(
        text(
            f"""
            SELECT cd_grupo, cd_cota, versao, parcela, mes_ano_comissao
            FROM {CONSORCIO_PAID_TABLE}
            """
        ),
        engine,
    )
    if rows.empty:
        return {}
    keys = (
        rows["cd_grupo"].fillna("").astype(str).str.strip().str.upper()
        + "||"
        + rows["cd_cota"].fillna("").astype(str).str.strip().str.upper()
        + "||"
        + rows["versao"].fillna("").astype(str).str.strip().str.upper()
        + "||"
        + rows["parcela"].fillna("").astype(str).str.split().str.join(" ").str.upper()
    )
    return dict(zip(keys, rows["mes_ano_comissao"].fillna("").astype(str)))


def read_consorcio_paid(period_label: str | None = None) -> pd.DataFrame:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    if period_label:
        query = text(
            f"SELECT * FROM {CONSORCIO_PAID_TABLE} WHERE mes_ano_comissao = :period ORDER BY filial, cen, cd_cota"
        )
        return pd.read_sql(query, engine, params={"period": period_label})
    return pd.read_sql(text(f"SELECT * FROM {CONSORCIO_PAID_TABLE} ORDER BY id"), engine)


def read_consorcio_paid_period_labels() -> list[str]:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    rows = pd.read_sql(
        text(
            f"""
            SELECT DISTINCT competencia_ano, competencia_mes, mes_ano_comissao
            FROM {CONSORCIO_PAID_TABLE}
            ORDER BY competencia_ano DESC, competencia_mes DESC
            """
        ),
        engine,
    )
    return rows["mes_ano_comissao"].fillna("").astype(str).tolist()
