"""PostgreSQL persistence for paid commissions and model rules."""

from __future__ import annotations

import os
from datetime import date
from urllib.parse import quote_plus

import pandas as pd

from commission_tool.core.formatting import (
    parse_br_number,
    parse_commission_percent_points,
    parse_margin_target_percent_points,
    parse_percent_points,
)


PAID_COMMISSIONS_TABLE = "comissoespagas"
EXCLUDED_COMMISSIONS_TABLE = "comissoesexcluidas"
FAT_RATE_TABLE = "comissao_faturamento_modelo"
MARGIN_RATE_TABLE = "comissao_margem_modelo"
MANAGER_RELATION_TABLE = "comissao_gerente_vendedor"
INCENTIVE_TITLES_TABLE = "commission_incentive_titles"
DEFAULT_MANAGER_COMMISSION_PERCENT = 33.0


PAID_COLUMN_MAP = {
    "Tipo": "tipo",
    "Filial": "filial",
    "Data de Emissão": "data_emissao",
    "Nro Documento": "nro_documento",
    "Modelo": "modelo",
    "Produto Código": "produto_codigo",
    "Nro Chassi": "nro_chassi",
    "Nome do Cliente": "nome_cliente",
    "CEN": "cen",
    "Cod Vendedor": "cod_vendedor",
    "Gerente": "gerente",
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
    "% ComissÃ£o Gerente": "perc_comissao_gerente",
    "Valor ComissÃ£o Gerente": "valor_comissao_gerente",
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
                    produto_codigo TEXT,
                    nro_chassi TEXT,
                    nome_cliente TEXT,
                    cen TEXT,
                    cod_vendedor TEXT,
                    gerente TEXT,
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
                    valor_comissao_total NUMERIC(18, 2),
                    perc_comissao_gerente NUMERIC(12, 6),
                    valor_comissao_gerente NUMERIC(18, 2)
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {EXCLUDED_COMMISSIONS_TABLE} (
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
                    produto_codigo TEXT,
                    nro_chassi TEXT,
                    nome_cliente TEXT,
                    cen TEXT,
                    cod_vendedor TEXT,
                    gerente TEXT,
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
                    valor_comissao_total NUMERIC(18, 2),
                    perc_comissao_gerente NUMERIC(12, 6),
                    valor_comissao_gerente NUMERIC(18, 2)
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
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {MANAGER_RELATION_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    filial TEXT,
                    gerente TEXT NOT NULL,
                    cod_vendedor TEXT NOT NULL,
                    cod_x TEXT,
                    vendedor TEXT,
                    data_nascimento TEXT,
                    cpf TEXT,
                    email TEXT,
                    email_gerente TEXT,
                    contato TEXT,
                    percentual_comissao_gerente NUMERIC(12, 6) NOT NULL DEFAULT 33,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(text(f"ALTER TABLE {FAT_RATE_TABLE} DROP CONSTRAINT IF EXISTS {FAT_RATE_TABLE}_modelo_key"))
        conn.execute(text(f"ALTER TABLE {MARGIN_RATE_TABLE} DROP CONSTRAINT IF EXISTS {MARGIN_RATE_TABLE}_modelo_key"))
        conn.execute(text(f"ALTER TABLE {MANAGER_RELATION_TABLE} DROP CONSTRAINT IF EXISTS {MANAGER_RELATION_TABLE}_cod_vendedor_key"))
        conn.execute(text(f"ALTER TABLE {MANAGER_RELATION_TABLE} ADD COLUMN IF NOT EXISTS email_gerente TEXT"))
        conn.execute(text(f"ALTER TABLE {PAID_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS cod_vendedor TEXT"))
        conn.execute(text(f"ALTER TABLE {PAID_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS gerente TEXT"))
        conn.execute(text(f"ALTER TABLE {PAID_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS perc_comissao_gerente NUMERIC(12, 6)"))
        conn.execute(text(f"ALTER TABLE {PAID_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS valor_comissao_gerente NUMERIC(18, 2)"))
        conn.execute(text(f"ALTER TABLE {EXCLUDED_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS cod_vendedor TEXT"))
        conn.execute(text(f"ALTER TABLE {EXCLUDED_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS gerente TEXT"))
        conn.execute(text(f"ALTER TABLE {EXCLUDED_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS perc_comissao_gerente NUMERIC(12, 6)"))
        conn.execute(text(f"ALTER TABLE {EXCLUDED_COMMISSIONS_TABLE} ADD COLUMN IF NOT EXISTS valor_comissao_gerente NUMERIC(18, 2)"))
        for table_name in (PAID_COMMISSIONS_TABLE, EXCLUDED_COMMISSIONS_TABLE):
            conn.execute(
                text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS produto_codigo TEXT")
            )
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
                CREATE INDEX IF NOT EXISTS idx_{EXCLUDED_COMMISSIONS_TABLE}_competencia
                ON {EXCLUDED_COMMISSIONS_TABLE} (competencia_ano, competencia_mes)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{EXCLUDED_COMMISSIONS_TABLE}_chassi
                ON {EXCLUDED_COMMISSIONS_TABLE} (nro_chassi)
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
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{MANAGER_RELATION_TABLE}_cod_vendedor
                ON {MANAGER_RELATION_TABLE} (cod_vendedor)
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
    if "perc_comissao_gerente" not in renamed.columns:
        manager_pct_col = next(
            (
                column
                for column in renamed.columns
                if "%" in str(column) and "gerente" in str(column).strip().lower()
            ),
            None,
        )
        if manager_pct_col is not None:
            renamed["perc_comissao_gerente"] = renamed[manager_pct_col]
    if "valor_comissao_gerente" not in renamed.columns:
        manager_value_col = next(
            (
                column
                for column in renamed.columns
                if "valor" in str(column).strip().lower() and "gerente" in str(column).strip().lower()
            ),
            None,
        )
        if manager_value_col is not None:
            renamed["valor_comissao_gerente"] = renamed[manager_value_col]

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
        "valor_comissao_gerente",
    ]
    percent_columns = [
        "perc_comissao_fat",
        "perc_margem_direta",
        "meta_margem",
        "perc_margem_bruta",
        "perc_comissao_margem",
        "perc_comissao_gerente",
    ]
    for column in amount_columns:
        prepared[column] = prepared[column].apply(parse_br_number)
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    for column in percent_columns:
        parser = parse_margin_target_percent_points if column == "meta_margem" else parse_percent_points
        prepared[column] = prepared[column].apply(parser)
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    return prepared


def _save_commission_rows(
    table_name: str,
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
        prepared.to_sql(table_name, conn, if_exists="append", index=False)
    return len(prepared)


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
    return _save_commission_rows(
        PAID_COMMISSIONS_TABLE,
        df,
        competence_year,
        competence_month,
        period_label,
        period_start,
        period_end,
        source,
        file_name,
    )


def save_excluded_commissions(
    df: pd.DataFrame,
    competence_year: int,
    competence_month: int,
    period_label: str,
    period_start: date | None = None,
    period_end: date | None = None,
    source: str = "streamlit_exclusao",
    file_name: str | None = None,
) -> int:
    return _save_commission_rows(
        EXCLUDED_COMMISSIONS_TABLE,
        df,
        competence_year,
        competence_month,
        period_label,
        period_start,
        period_end,
        source,
        file_name,
    )


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


def read_paid_commission_period_labels() -> list[str]:
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT mes_ano_comissao
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE mes_ano_comissao IS NOT NULL
          AND TRIM(mes_ano_comissao) <> ''
        GROUP BY mes_ano_comissao
        ORDER BY MAX(competencia_ano) DESC, MAX(competencia_mes) DESC, mes_ano_comissao DESC
        """
    )
    df = pd.read_sql(query, engine)
    return df["mes_ano_comissao"].astype(str).tolist() if not df.empty else []


MANUAL_PAID_COMMISSION_FIELDS = [
    "tipo",
    "filial",
    "data_emissao",
    "nro_documento",
    "modelo",
    "nro_chassi",
    "nome_cliente",
    "cen",
    "cod_vendedor",
    "gerente",
    "classificacao_venda",
    "receita_bruta",
    "perc_comissao_fat",
    "valor_comissao_fat",
    "cmv",
    "margem_rs",
    "perc_margem_direta",
    "valor_incentivo",
    "receita_bruta_incentivos_rs",
    "margem_incentivos_rs",
    "meta_margem",
    "perc_margem_bruta",
    "perc_comissao_margem",
    "valor_comissao_margem",
    "valor_comissao_total",
    "perc_comissao_gerente",
    "valor_comissao_gerente",
]


def insert_manual_paid_commission(
    values: dict,
    *,
    competence_year: int,
    competence_month: int,
    period_label: str,
    period_start: date | None,
    period_end: date | None,
    source: str = "streamlit_manual",
    file_name: str | None = None,
) -> int:
    """Insert a single row in comissoespagas directly, bypassing Excel-style parsing.

    `values` must already contain typed values (floats for amounts/percentages, date for
    data_emissao, strings elsewhere). Unknown keys are ignored; missing keys become NULL.
    Returns 1 on success.
    """
    from sqlalchemy import text

    ensure_commission_tables()

    payload: dict[str, object] = {field: values.get(field) for field in MANUAL_PAID_COMMISSION_FIELDS}
    payload.update(
        {
            "competencia_ano": competence_year,
            "competencia_mes": competence_month,
            "mes_ano_comissao": period_label,
            "periodo_inicio": period_start,
            "periodo_fim": period_end,
            "fonte": source or "streamlit_manual",
            "arquivo_origem": file_name,
        }
    )

    columns = [
        "competencia_ano",
        "competencia_mes",
        "mes_ano_comissao",
        "periodo_inicio",
        "periodo_fim",
        "fonte",
        "arquivo_origem",
        *MANUAL_PAID_COMMISSION_FIELDS,
    ]
    column_sql = ", ".join(columns)
    placeholders_sql = ", ".join(f":{col}" for col in columns)

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                f"INSERT INTO {PAID_COMMISSIONS_TABLE} ({column_sql}) VALUES ({placeholders_sql})"
            ),
            payload,
        )
    return 1


def read_manual_paid_commissions(period_label: str | None = None) -> pd.DataFrame:
    """Read all manual entries (fonte ILIKE '%manual%'), optionally filtered by competência."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()

    where_clauses = ["fonte ILIKE '%manual%'"]
    params: dict = {}
    if period_label:
        where_clauses.append("mes_ano_comissao = :period_label")
        params["period_label"] = period_label

    where_sql = " AND ".join(where_clauses)
    query = text(
        f"""
        SELECT *
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE {where_sql}
        ORDER BY mes_ano_comissao, paid_at DESC, id DESC
        """
    )
    return pd.read_sql(query, engine, params=params)


def update_manual_paid_commission(
    row_id: int,
    values: dict,
    *,
    competence_year: int,
    competence_month: int,
    period_label: str,
    period_start: date | None,
    period_end: date | None,
    source: str | None = None,
    file_name: str | None = None,
) -> int:
    """Update a single row in comissoespagas, identified by id. Returns rowcount."""
    from sqlalchemy import text

    ensure_commission_tables()

    payload: dict[str, object] = {field: values.get(field) for field in MANUAL_PAID_COMMISSION_FIELDS}
    payload.update(
        {
            "competencia_ano": competence_year,
            "competencia_mes": competence_month,
            "mes_ano_comissao": period_label,
            "periodo_inicio": period_start,
            "periodo_fim": period_end,
            "fonte": source if source is not None else None,
            "arquivo_origem": file_name,
            "row_id": row_id,
        }
    )

    set_columns = [
        "competencia_ano",
        "competencia_mes",
        "mes_ano_comissao",
        "periodo_inicio",
        "periodo_fim",
        "arquivo_origem",
        *MANUAL_PAID_COMMISSION_FIELDS,
    ]
    if source is not None:
        set_columns.insert(5, "fonte")  # only update fonte when caller provided one
    set_sql = ", ".join(f"{col} = :{col}" for col in set_columns)

    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"UPDATE {PAID_COMMISSIONS_TABLE} SET {set_sql} WHERE id = :row_id"
            ),
            payload,
        )
    return result.rowcount or 0


def delete_paid_commission_by_id(row_id: int) -> int:
    """Hard delete a single comissoespagas row by id. Returns rowcount."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {PAID_COMMISSIONS_TABLE} WHERE id = :row_id"),
            {"row_id": row_id},
        )
    return result.rowcount or 0


def delete_paid_commissions_by_ids(row_ids: list[int]) -> int:
    """Hard delete multiple comissoespagas rows by id in a single transaction.

    Used for the "rollback" flow on the Comissões pagas screen — removes selected
    rows so the corresponding chassi+documento pair returns to the apuração as a
    candidate (no longer blocked by paid history).
    """
    from sqlalchemy import text

    ensure_commission_tables()
    cleaned_ids = sorted({int(row_id) for row_id in row_ids if row_id is not None})
    if not cleaned_ids:
        return 0

    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {PAID_COMMISSIONS_TABLE} WHERE id = ANY(:ids)"),
            {"ids": cleaned_ids},
        )
    return result.rowcount or 0


def read_paid_commissions_by_chassis(chassis: list[str]) -> pd.DataFrame:
    """Read every paid-commission row whose normalized chassis is in the given list."""
    from sqlalchemy import text

    ensure_commission_tables()
    normalized = sorted({str(value).strip().upper() for value in chassis if str(value).strip()})
    if not normalized:
        return pd.DataFrame()

    engine = get_engine()
    query = text(
        f"""
        SELECT *
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE UPPER(TRIM(nro_chassi)) = ANY(:chassis)
        ORDER BY UPPER(TRIM(nro_chassi)), competencia_ano, competencia_mes, paid_at, id
        """
    )
    return pd.read_sql(query, engine, params={"chassis": normalized})


def read_paid_commissions_by_period_label(period_label: str) -> pd.DataFrame:
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT *
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE mes_ano_comissao = :period_label
        ORDER BY paid_at DESC, id DESC
        """
    )
    return pd.read_sql(query, engine, params={"period_label": period_label})


def read_paid_commission_chassis_summary() -> pd.DataFrame:
    """Read all paid commission history aggregated by chassis."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT
            UPPER(TRIM(nro_chassi)) AS nro_chassi,
            COUNT(*) AS qtd_lancamentos_pagos,
            COALESCE(SUM(CASE WHEN valor_comissao_total > 0 THEN valor_comissao_total ELSE 0 END), 0)
                AS valor_pago_positivo,
            COALESCE(SUM(CASE WHEN valor_comissao_total < 0 THEN valor_comissao_total ELSE 0 END), 0)
                AS valor_estornado_negativo,
            COALESCE(SUM(valor_comissao_total), 0) AS saldo_comissao_paga_chassi,
            BOOL_OR(COALESCE(valor_comissao_total, 0) > 0) AS tem_pagamento_positivo,
            BOOL_OR(COALESCE(valor_comissao_total, 0) < 0) AS tem_estorno_negativo
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE nro_chassi IS NOT NULL
          AND TRIM(nro_chassi) <> ''
        GROUP BY UPPER(TRIM(nro_chassi))
        """
    )
    return pd.read_sql(query, engine)


def read_paid_commission_document_summary() -> pd.DataFrame:
    """Read all paid commission history aggregated by chassis and document.

    Rows without chassis are kept: for them the document alone is the key.
    """
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT
            COALESCE(UPPER(TRIM(nro_chassi)), '') AS nro_chassi,
            TRIM(nro_documento) AS nro_documento,
            COUNT(*) AS qtd_lancamentos_documento_pago,
            COALESCE(SUM(valor_comissao_total), 0) AS valor_comissao_total_documento
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE nro_documento IS NOT NULL
          AND TRIM(nro_documento) <> ''
        GROUP BY COALESCE(UPPER(TRIM(nro_chassi)), ''), TRIM(nro_documento)
        """
    )
    return pd.read_sql(query, engine)


def read_excluded_commission_chassis_summary() -> pd.DataFrame:
    """Read all manually excluded commission history aggregated by chassis."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT
            UPPER(TRIM(nro_chassi)) AS nro_chassi,
            COUNT(*) AS qtd_lancamentos_excluidos,
            MAX(paid_at) AS ultima_exclusao
        FROM {EXCLUDED_COMMISSIONS_TABLE}
        WHERE nro_chassi IS NOT NULL
          AND TRIM(nro_chassi) <> ''
        GROUP BY UPPER(TRIM(nro_chassi))
        """
    )
    return pd.read_sql(query, engine)


def read_excluded_commission_document_summary() -> pd.DataFrame:
    """Read manually excluded commissions aggregated by chassis and document."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT
            COALESCE(UPPER(TRIM(nro_chassi)), '') AS nro_chassi,
            TRIM(nro_documento) AS nro_documento,
            COUNT(*) AS qtd_lancamentos_excluidos
        FROM {EXCLUDED_COMMISSIONS_TABLE}
        WHERE nro_documento IS NOT NULL
          AND TRIM(nro_documento) <> ''
        GROUP BY COALESCE(UPPER(TRIM(nro_chassi)), ''), TRIM(nro_documento)
        """
    )
    return pd.read_sql(query, engine)


def read_model_fat_rates() -> pd.DataFrame:
    """Read the latest active faturamento rate per model."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT DISTINCT ON (UPPER(TRIM(modelo)))
            id,
            grupo,
            modelo,
            CASE WHEN ABS(percentual) > 10 THEN percentual / 100 ELSE percentual END AS percentual,
            ativo,
            updated_at
        FROM {FAT_RATE_TABLE}
        WHERE ativo IS TRUE
          AND modelo IS NOT NULL
          AND TRIM(modelo) <> ''
        ORDER BY UPPER(TRIM(modelo)), updated_at DESC, id DESC
        """
    )
    return pd.read_sql(query, engine)


def read_model_margin_rates() -> pd.DataFrame:
    """Read the latest active margin rate and margin target per model."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT DISTINCT ON (UPPER(TRIM(modelo)))
            id,
            grupo,
            modelo,
            CASE WHEN ABS(percentual) > 10 THEN percentual / 100 ELSE percentual END AS percentual,
            CASE
                WHEN ABS(meta_margem) <= 1 AND meta_margem <> 0 THEN meta_margem * 100
                ELSE meta_margem
            END AS meta_margem,
            ativo,
            updated_at
        FROM {MARGIN_RATE_TABLE}
        WHERE ativo IS TRUE
          AND modelo IS NOT NULL
          AND TRIM(modelo) <> ''
        ORDER BY UPPER(TRIM(modelo)), updated_at DESC, id DESC
        """
    )
    return pd.read_sql(query, engine)


def read_manager_relations() -> pd.DataFrame:
    """Read the latest active manager relation per seller code."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT DISTINCT ON (UPPER(TRIM(cod_vendedor)))
            id,
            filial,
            gerente,
            cod_vendedor,
            cod_x,
            vendedor,
            data_nascimento,
            cpf,
            email,
            email_gerente,
            contato,
            CASE
                WHEN ABS(percentual_comissao_gerente) <= 1 AND percentual_comissao_gerente <> 0
                    THEN percentual_comissao_gerente * 100
                ELSE percentual_comissao_gerente
            END AS percentual_comissao_gerente,
            ativo,
            updated_at
        FROM {MANAGER_RELATION_TABLE}
        WHERE ativo IS TRUE
          AND cod_vendedor IS NOT NULL
          AND TRIM(cod_vendedor) <> ''
        ORDER BY UPPER(TRIM(cod_vendedor)), updated_at DESC, id DESC
        """
    )
    return pd.read_sql(query, engine)


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
    prepared["percentual"] = prepared["percentual"].apply(parse_commission_percent_points)
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


def replace_active_model_fat_rates(df: pd.DataFrame) -> int:
    """Deactivate current active faturamento rates and append a new active version."""
    ensure_commission_tables()
    prepared = _prepare_model_fat_rates(df)
    engine = get_engine()
    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(f"UPDATE {FAT_RATE_TABLE} SET ativo = FALSE WHERE ativo IS TRUE"))
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
        "Meta", "meta",
        "Meta de Margem", "Meta Margem", "meta_margem",
        "% Meta de Margem", "% Meta Margem", "Meta de Margem %",
        "Meta Margem %", "meta margem", "meta de margem",
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
    prepared["percentual"] = prepared["percentual"].apply(parse_commission_percent_points)
    if "grupo" not in prepared.columns:
        prepared["grupo"] = None
    if "meta_margem" not in prepared.columns:
        prepared["meta_margem"] = 0
    prepared["meta_margem"] = prepared["meta_margem"].apply(parse_margin_target_percent_points)
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


def replace_active_model_margin_rates(df: pd.DataFrame) -> int:
    """Deactivate current active margin rates and append a new active version."""
    ensure_commission_tables()
    prepared = _prepare_model_margin_rates(df)
    engine = get_engine()
    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(f"UPDATE {MARGIN_RATE_TABLE} SET ativo = FALSE WHERE ativo IS TRUE"))
        prepared.to_sql(MARGIN_RATE_TABLE, conn, if_exists="append", index=False)
    return len(prepared)


def _prepare_manager_relations(df: pd.DataFrame) -> pd.DataFrame:
    filial_col = _find_column(df, ["Filial", "filial"], required=False)
    gerente_col = _find_column(df, ["Gerente", "gerente"])
    cod_vendedor_col = _find_column(
        df,
        ["Cod Vendedor", "cod_vendedor", "CodVendedor", "Código Vendedor", "Codigo Vendedor"],
    )
    cod_x_col = _find_column(df, ["Cod X", "cod_x", "CodX"], required=False)
    vendedor_col = _find_column(df, ["Vendedor", "vendedor"], required=False)
    nascimento_col = _find_column(
        df,
        ["Data de Nascimento", "data_nascimento", "Nascimento"],
        required=False,
    )
    cpf_col = _find_column(df, ["CPF", "cpf"], required=False)
    email_col = _find_column(df, ["E-mail", "Email", "email"], required=False)
    manager_email_col = _find_column(
        df,
        ["E-mail Gerente", "Email Gerente", "email_gerente", "e-mail_gerente"],
        required=False,
    )
    contato_col = _find_column(df, ["Contato", "contato"], required=False)
    pct_col = _find_column(
        df,
        [
            "% Comissão Gerente",
            "% Comissao Gerente",
            "Percentual Comissão Gerente",
            "Percentual Comissao Gerente",
            "percentual_comissao_gerente",
        ],
        required=False,
    )

    prepared = pd.DataFrame(
        {
            "filial": df[filial_col] if filial_col else None,
            "gerente": df[gerente_col],
            "cod_vendedor": df[cod_vendedor_col],
            "cod_x": df[cod_x_col] if cod_x_col else None,
            "vendedor": df[vendedor_col] if vendedor_col else None,
            "data_nascimento": df[nascimento_col] if nascimento_col else None,
            "cpf": df[cpf_col] if cpf_col else None,
            "email": df[email_col] if email_col else None,
            "email_gerente": df[manager_email_col] if manager_email_col else None,
            "contato": df[contato_col] if contato_col else None,
            "percentual_comissao_gerente": (
                df[pct_col] if pct_col else DEFAULT_MANAGER_COMMISSION_PERCENT
            ),
        }
    )
    prepared["cod_vendedor"] = prepared["cod_vendedor"].fillna("").astype(str).str.strip().str.upper()
    prepared["gerente"] = prepared["gerente"].fillna("").astype(str).str.strip()
    prepared = prepared[prepared["cod_vendedor"].ne("") & prepared["gerente"].ne("")]
    prepared["percentual_comissao_gerente"] = prepared["percentual_comissao_gerente"].apply(parse_percent_points)
    prepared["ativo"] = True
    return prepared.reset_index(drop=True)


def replace_active_manager_relations(df: pd.DataFrame) -> int:
    """Deactivate current active manager relations and append a new active version."""
    ensure_commission_tables()
    prepared = _prepare_manager_relations(df)
    engine = get_engine()
    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(f"UPDATE {MANAGER_RELATION_TABLE} SET ativo = FALSE WHERE ativo IS TRUE"))
        prepared.to_sql(MANAGER_RELATION_TABLE, conn, if_exists="append", index=False)
    return len(prepared)
