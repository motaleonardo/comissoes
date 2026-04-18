"""PostgreSQL persistence for paid commissions and model rules."""

from __future__ import annotations

import os
from datetime import date
from urllib.parse import quote_plus

import pandas as pd

from commission_tool.core.formatting import parse_br_number, parse_percent_points


PAID_COMMISSIONS_TABLE = "comissoespagas"
FAT_RATE_TABLE = "comissao_faturamento_modelo"
MARGIN_RATE_TABLE = "comissao_margem_modelo"
INCENTIVE_TITLES_TABLE = "commission_incentive_titles"


PAID_COLUMN_MAP = {
    "Tipo": "tipo",
    "Filial": "filial",
    "Data de Emissão": "data_emissao",
    "Nro Documento": "nro_documento",
    "Modelo": "modelo",
    "Nro Chassi": "nro_chassi",
    "Nome do Cliente": "nome_cliente",
    "CEN": "cen",
    "Classificação Venda": "classificacao_venda",
    "Receita Bruta": "receita_bruta",
    "% Comissão Fat.": "perc_comissao_fat",
    "Valor Comissão Fat.": "valor_comissao_fat",
    "CMV": "cmv",
    "Margem R$": "margem_rs",
    "% Margem Direta": "perc_margem_direta",
    "Valor Incentivo": "valor_incentivo",
    "Receita Bruta + Incentivos R$": "receita_bruta_incentivos_rs",
    "Margem + Incentivos R$": "margem_incentivos_rs",
    "Meta de Margem": "meta_margem",
    "% Margem Bruta": "perc_margem_bruta",
    "% Comissão Margem": "perc_comissao_margem",
    "Valor Comissão Margem": "valor_comissao_margem",
    "Valor Comissão Total": "valor_comissao_total",
}


def get_postgres_url() -> str:
    explicit_url = os.getenv("POSTGRES_URL", "").strip()
    if explicit_url:
        # Normalize common URL schemes to the SQLAlchemy psycopg dialect
        if explicit_url.startswith("postgres://"):
            explicit_url = explicit_url.replace("postgres://", "postgresql+psycopg://", 1)
        elif explicit_url.startswith("postgresql://"):
            explicit_url = explicit_url.replace("postgresql://", "postgresql+psycopg://", 1)
        return explicit_url

    host = os.getenv("POSTGRES_HOST", "").strip()
    database = os.getenv("POSTGRES_DB", "").strip()
    user = os.getenv("POSTGRES_USER", "").strip()
    password = os.getenv("POSTGRES_PASSWORD", "").strip()
    port = os.getenv("POSTGRES_PORT", "5432").strip()
    sslmode = os.getenv("POSTGRES_SSLMODE", "").strip()

    if not all([host, database, user]):
        return ""

    auth = quote_plus(user)
    if password:
        auth += f":{quote_plus(password)}"

    url = f"postgresql+psycopg://{auth}@{host}:{port}/{database}"
    if sslmode:
        url += f"?sslmode={quote_plus(sslmode)}"
    return url


def get_engine():
    postgres_url = get_postgres_url()
    if not postgres_url:
        raise RuntimeError("Configure POSTGRES_URL ou POSTGRES_HOST/POSTGRES_DB/POSTGRES_USER no .env.")

    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        raise RuntimeError("Instale sqlalchemy e psycopg para usar o Postgres.") from exc

    return create_engine(postgres_url)


def ensure_commission_tables() -> None:
    """Create the three commission tables when they do not exist."""
    from sqlalchemy import text

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {PAID_COMMISSIONS_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    paid_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    competencia_ano INTEGER NOT NULL,
                    competencia_mes INTEGER NOT NULL,
                    mes_ano_comissao TEXT NOT NULL,
                    periodo_inicio DATE,
                    periodo_fim DATE,
                    fonte TEXT NOT NULL DEFAULT 'streamlit',
                    arquivo_origem TEXT,
                    tipo TEXT,
                    filial TEXT,
                    data_emissao DATE,
                    nro_documento TEXT,
                    modelo TEXT,
                    nro_chassi TEXT,
                    nome_cliente TEXT,
                    cen TEXT,
                    classificacao_venda TEXT,
                    receita_bruta NUMERIC(18, 2),
                    perc_comissao_fat NUMERIC(12, 6),
                    valor_comissao_fat NUMERIC(18, 2),
                    cmv NUMERIC(18, 2),
                    margem_rs NUMERIC(18, 2),
                    perc_margem_direta NUMERIC(12, 6),
                    valor_incentivo NUMERIC(18, 2),
                    receita_bruta_incentivos_rs NUMERIC(18, 2),
                    margem_incentivos_rs NUMERIC(18, 2),
                    meta_margem NUMERIC(12, 6),
                    perc_margem_bruta NUMERIC(12, 6),
                    perc_comissao_margem NUMERIC(12, 6),
                    valor_comissao_margem NUMERIC(18, 2),
                    valor_comissao_total NUMERIC(18, 2)
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {FAT_RATE_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    grupo TEXT,
                    modelo TEXT NOT NULL,
                    percentual NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {MARGIN_RATE_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    grupo TEXT,
                    modelo TEXT NOT NULL,
                    percentual NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    meta_margem NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(text(f"ALTER TABLE {FAT_RATE_TABLE} DROP CONSTRAINT IF EXISTS {FAT_RATE_TABLE}_modelo_key"))
        conn.execute(text(f"ALTER TABLE {MARGIN_RATE_TABLE} DROP CONSTRAINT IF EXISTS {MARGIN_RATE_TABLE}_modelo_key"))
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{PAID_COMMISSIONS_TABLE}_competencia
                ON {PAID_COMMISSIONS_TABLE} (competencia_ano, competencia_mes)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{PAID_COMMISSIONS_TABLE}_chassi
                ON {PAID_COMMISSIONS_TABLE} (nro_chassi)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{FAT_RATE_TABLE}_modelo
                ON {FAT_RATE_TABLE} (modelo)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{MARGIN_RATE_TABLE}_modelo
                ON {MARGIN_RATE_TABLE} (modelo)
                """
            )
        )


def expected_paid_commissions_columns() -> list[str]:
    return [
        "competencia_ano",
        "competencia_mes",
        "mes_ano_comissao",
        "periodo_inicio",
        "periodo_fim",
        "fonte",
        "arquivo_origem",
        *PAID_COLUMN_MAP.values(),
    ]


def get_paid_commissions_schema_status() -> pd.DataFrame:
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = :table_name
                """
            ),
            {"table_name": PAID_COMMISSIONS_TABLE},
        ).fetchall()

    existing = {row[0] for row in rows}
    expected = expected_paid_commissions_columns()
    return pd.DataFrame(
        [
            {
                "coluna": column,
                "status": "OK" if column in existing else "Ausente",
            }
            for column in expected
        ]
    )


def _prepare_paid_commissions_df(
    df: pd.DataFrame,
    competence_year: int,
    competence_month: int,
    period_label: str,
    period_start: date | None,
    period_end: date | None,
    source: str,
    file_name: str | None = None,
) -> pd.DataFrame:
    renamed = df.rename(columns=PAID_COLUMN_MAP).copy()
    keep_columns = list(PAID_COLUMN_MAP.values())
    for column in keep_columns:
        if column not in renamed.columns:
            renamed[column] = None

    prepared = renamed[keep_columns].copy()
    prepared.insert(0, "arquivo_origem", file_name)
    prepared.insert(0, "fonte", source)
    prepared.insert(0, "periodo_fim", period_end)
    prepared.insert(0, "periodo_inicio", period_start)
    prepared.insert(0, "mes_ano_comissao", period_label)
    prepared.insert(0, "competencia_mes", competence_month)
    prepared.insert(0, "competencia_ano", competence_year)

    if "data_emissao" in prepared.columns:
        prepared["data_emissao"] = pd.to_datetime(prepared["data_emissao"], dayfirst=True, errors="coerce").dt.date

    amount_columns = [
        "receita_bruta",
        "valor_comissao_fat",
        "cmv",
        "margem_rs",
        "valor_incentivo",
        "receita_bruta_incentivos_rs",
        "margem_incentivos_rs",
        "valor_comissao_margem",
        "valor_comissao_total",
    ]
    percent_columns = [
        "perc_comissao_fat",
        "perc_margem_direta",
        "meta_margem",
        "perc_margem_bruta",
        "perc_comissao_margem",
    ]
    for column in amount_columns:
        prepared[column] = prepared[column].apply(parse_br_number)
    for column in percent_columns:
        prepared[column] = prepared[column].apply(parse_percent_points)

    return prepared


def save_paid_commissions(
    df: pd.DataFrame,
    competence_year: int,
    competence_month: int,
    period_label: str,
    period_start: date | None = None,
    period_end: date | None = None,
    source: str = "streamlit",
    file_name: str | None = None,
) -> int:
    if df.empty:
        return 0

    ensure_commission_tables()
    prepared = _prepare_paid_commissions_df(
        df,
        competence_year,
        competence_month,
        period_label,
        period_start,
        period_end,
        source,
        file_name,
    )

    engine = get_engine()
    with engine.begin() as conn:
        prepared.to_sql(PAID_COMMISSIONS_TABLE, conn, if_exists="append", index=False)
    return len(prepared)


def read_paid_commissions(competence_year: int, competence_month: int) -> pd.DataFrame:
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT *
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE competencia_ano = :competence_year
          AND competencia_mes = :competence_month
        ORDER BY paid_at DESC, id DESC
        """
    )
    return pd.read_sql(
        query,
        engine,
        params={"competence_year": competence_year, "competence_month": competence_month},
    )


def save_incentive_titles(df: pd.DataFrame, table_name: str = INCENTIVE_TITLES_TABLE) -> None:
    """Persist incentive title checks outside the main commission table."""
    ensure_commission_tables()
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)


def _find_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    """Find a DataFrame column by trying exact matches, then case-insensitive/stripped matches."""
    cols = list(df.columns)

    # Exact match
    for candidate in candidates:
        if candidate in cols:
            return candidate

    # Case-insensitive + stripped match
    cols_lower = {col.strip().lower(): col for col in cols}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in cols_lower:
            return cols_lower[key]

    if required:
        raise KeyError(
            f"Nenhuma coluna encontrada para {candidates}. "
            f"Colunas disponíveis: {cols}"
        )
    return None


def _prepare_model_fat_rates(df: pd.DataFrame) -> pd.DataFrame:
    grupo_col = _find_column(df, ["Grupo", "grupo", "GRUPO"], required=False)
    modelo_col = _find_column(df, ["Modelo", "modelo", "MODELO"])
    pct_col = _find_column(df, [
        "Percentual", "percentual", "PERCENTUAL",
        "% Comissão Fat.", "% Comissão Fat", "% Comissao Fat.",
        "% Comissao Fat", "percentual_comissao_fat",
    ])

    data = {
        "modelo": df[modelo_col],
        "percentual": df[pct_col],
    }
    if grupo_col:
        data["grupo"] = df[grupo_col]

    prepared = pd.DataFrame(data).dropna(subset=["modelo"])
    prepared["percentual"] = prepared["percentual"].apply(parse_percent_points)
    if "grupo" not in prepared.columns:
        prepared["grupo"] = None
    prepared["ativo"] = True
    return prepared


def save_model_fat_rates(df: pd.DataFrame) -> int:
    """Replace faturamento commission percentages by model."""
    ensure_commission_tables()
    prepared = _prepare_model_fat_rates(df)

    engine = get_engine()
    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(f"DELETE FROM {FAT_RATE_TABLE}"))
        prepared.to_sql(FAT_RATE_TABLE, conn, if_exists="append", index=False)
    return len(prepared)


def append_model_fat_rates(df: pd.DataFrame) -> int:
    """Append faturamento commission percentages by model, preserving duplicates."""
    ensure_commission_tables()
    prepared = _prepare_model_fat_rates(df)
    engine = get_engine()
    with engine.begin() as conn:
        prepared.to_sql(FAT_RATE_TABLE, conn, if_exists="append", index=False)
    return len(prepared)


def _prepare_model_margin_rates(df: pd.DataFrame) -> pd.DataFrame:
    grupo_col = _find_column(df, ["Grupo", "grupo", "GRUPO"], required=False)
    modelo_col = _find_column(df, ["Modelo", "modelo", "MODELO"])
    pct_col = _find_column(df, [
        "Percentual", "percentual", "PERCENTUAL",
        "% Comissão Margem", "% Comissão Margem.", "% Comissao Margem",
        "percentual_comissao_margem",
    ])
    meta_col = _find_column(df, [
        "Meta de Margem", "Meta Margem", "meta_margem",
    ], required=False)

    data = {
        "modelo": df[modelo_col],
        "percentual": df[pct_col],
    }
    if grupo_col:
        data["grupo"] = df[grupo_col]
    if meta_col:
        data["meta_margem"] = df[meta_col]

    prepared = pd.DataFrame(data).dropna(subset=["modelo"])
    prepared["percentual"] = prepared["percentual"].apply(parse_percent_points)
    if "grupo" not in prepared.columns:
        prepared["grupo"] = None
    if "meta_margem" not in prepared.columns:
        prepared["meta_margem"] = 0
    prepared["meta_margem"] = prepared["meta_margem"].apply(parse_percent_points)
    prepared["ativo"] = True
    return prepared


def save_model_margin_rates(df: pd.DataFrame) -> int:
    """Replace margin commission percentages and margin targets by model."""
    ensure_commission_tables()
    prepared = _prepare_model_margin_rates(df)

    engine = get_engine()
    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(f"DELETE FROM {MARGIN_RATE_TABLE}"))
        prepared.to_sql(MARGIN_RATE_TABLE, conn, if_exists="append", index=False)
    return len(prepared)


def append_model_margin_rates(df: pd.DataFrame) -> int:
    """Append margin commission percentages by model, preserving duplicates."""
    ensure_commission_tables()
    prepared = _prepare_model_margin_rates(df)
    engine = get_engine()
    with engine.begin() as conn:
        prepared.to_sql(MARGIN_RATE_TABLE, conn, if_exists="append", index=False)
    return len(prepared)
