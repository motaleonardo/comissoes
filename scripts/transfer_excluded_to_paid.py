"""One-off data fix: move row(s) from comissoesexcluidas back to comissoespagas.

Use case: a chassi was sent to comissoesexcluidas (via "Excluir comissões" on the
apuração screen) and the operator decides to undo. This script copies the row(s)
to comissoespagas (preserving paid_at) and deletes from comissoesexcluidas, all
inside a single transaction.

Schema compatibility: both tables share the same 34 business columns. Confirmed
in commission_tool/data/sources/postgres.py (see CREATE TABLE blocks for
PAID_COMMISSIONS_TABLE and EXCLUDED_COMMISSIONS_TABLE). The `id` column is
BIGSERIAL in both — the inserted row gets a NEW id in comissoespagas.

Run preview (default — does NOT change anything):
    python scripts/transfer_excluded_to_paid.py

Apply:
    python scripts/transfer_excluded_to_paid.py --apply
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env")
except ImportError:
    pass

import pandas as pd
from sqlalchemy import text

from commission_tool.data.sources.postgres import (
    EXCLUDED_COMMISSIONS_TABLE,
    PAID_COMMISSIONS_TABLE,
    ensure_commission_tables,
    get_engine,
)


# Edit this list to transfer different ids from comissoesexcluidas.
EXCLUDED_IDS: list[int] = [111]


# Business columns shared by both tables. The id is intentionally excluded —
# the destination row gets a fresh BIGSERIAL id. paid_at is copied explicitly
# to preserve the original timestamp.
TRANSFER_COLUMNS: list[str] = [
    "paid_at",
    "competencia_ano",
    "competencia_mes",
    "mes_ano_comissao",
    "periodo_inicio",
    "periodo_fim",
    "fonte",
    "arquivo_origem",
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


def verify_schema_compatibility(engine) -> tuple[bool, list[str]]:
    """Check both tables have all TRANSFER_COLUMNS. Returns (ok, missing_report)."""
    query = text(
        """
        SELECT column_name, table_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name IN (:t1, :t2)
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(
            query, {"t1": PAID_COMMISSIONS_TABLE, "t2": EXCLUDED_COMMISSIONS_TABLE}
        ).fetchall()
    by_table: dict[str, set[str]] = {PAID_COMMISSIONS_TABLE: set(), EXCLUDED_COMMISSIONS_TABLE: set()}
    for column_name, table_name in rows:
        by_table[table_name].add(column_name)

    issues: list[str] = []
    for col in TRANSFER_COLUMNS:
        if col not in by_table[PAID_COMMISSIONS_TABLE]:
            issues.append(f"[{PAID_COMMISSIONS_TABLE}] está faltando coluna '{col}'")
        if col not in by_table[EXCLUDED_COMMISSIONS_TABLE]:
            issues.append(f"[{EXCLUDED_COMMISSIONS_TABLE}] está faltando coluna '{col}'")
    return (len(issues) == 0, issues)


def preview(engine, ids: Iterable[int]) -> pd.DataFrame:
    cleaned = sorted({int(i) for i in ids})
    if not cleaned:
        return pd.DataFrame()
    query = text(
        f"""
        SELECT id, paid_at, mes_ano_comissao, tipo, filial, nro_documento, nro_chassi,
               cen, cod_vendedor, gerente, classificacao_venda, valor_comissao_total, fonte
        FROM {EXCLUDED_COMMISSIONS_TABLE}
        WHERE id = ANY(:ids)
        ORDER BY id
        """
    )
    return pd.read_sql(query, engine, params={"ids": cleaned})


def transfer(engine, ids: Iterable[int]) -> tuple[int, int]:
    """Move rows from comissoesexcluidas to comissoespagas atomically.

    Returns (inserted, deleted).
    """
    cleaned = sorted({int(i) for i in ids})
    if not cleaned:
        return 0, 0

    columns_sql = ", ".join(TRANSFER_COLUMNS)
    insert_sql = text(
        f"""
        INSERT INTO {PAID_COMMISSIONS_TABLE} ({columns_sql})
        SELECT {columns_sql}
        FROM {EXCLUDED_COMMISSIONS_TABLE}
        WHERE id = ANY(:ids)
        """
    )
    delete_sql = text(
        f"DELETE FROM {EXCLUDED_COMMISSIONS_TABLE} WHERE id = ANY(:ids)"
    )

    with engine.begin() as conn:
        insert_result = conn.execute(insert_sql, {"ids": cleaned})
        delete_result = conn.execute(delete_sql, {"ids": cleaned})
        inserted = insert_result.rowcount or 0
        deleted = delete_result.rowcount or 0
        if inserted != deleted:
            # safety: if counts diverge, rollback (transaction scope ends on exception)
            raise RuntimeError(
                f"Inconsistência detectada: INSERT moveu {inserted} linha(s) mas DELETE removeu {deleted}. "
                "Rollback automático aplicado."
            )
    return inserted, deleted


MAX_ROWS_DEFAULT = 5


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Executa a transferência. Sem essa flag, apenas mostra o preview.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=MAX_ROWS_DEFAULT,
        help=(
            f"Aborta --apply se mais de N linhas seriam movidas (default: {MAX_ROWS_DEFAULT}). "
            "Salvaguarda contra transferências em massa acidentais."
        ),
    )
    args = parser.parse_args()

    ensure_commission_tables()
    engine = get_engine()

    print("=== 1) Verificação de compatibilidade de schema ===")
    ok, issues = verify_schema_compatibility(engine)
    if not ok:
        print("INCOMPATÍVEL — corrija o schema antes de prosseguir:")
        for issue in issues:
            print(f"  - {issue}")
        return 2
    print(f"OK. As {len(TRANSFER_COLUMNS)} colunas de negócio existem em ambas as tabelas.")
    print()

    print(f"=== 2) PREVIEW: linhas em {EXCLUDED_COMMISSIONS_TABLE} a serem movidas ===")
    df_preview = preview(engine, EXCLUDED_IDS)
    if df_preview.empty:
        print(f"Nenhum id encontrado em {EXCLUDED_COMMISSIONS_TABLE} para a lista: {EXCLUDED_IDS}.")
        return 0

    with pd.option_context("display.max_columns", None, "display.width", 220):
        print(df_preview.to_string(index=False))

    affected = len(df_preview)
    print()
    print(f"Total de linhas a transferir: {affected}")

    if not args.apply:
        print()
        print("Modo DRY-RUN. Nada foi alterado. Re-execute com --apply para confirmar.")
        return 0

    if affected > args.max_rows:
        print()
        print(
            f"ABORTADO: {affected} linhas seriam movidas, mas o limite é {args.max_rows} "
            f"(--max-rows). Revise EXCLUDED_IDS ou aumente o limite explicitamente."
        )
        return 2

    print()
    print(
        f"⚠️  Atenção: a transferência insere em {PAID_COMMISSIONS_TABLE} (com NOVO id) "
        f"e exclui de {EXCLUDED_COMMISSIONS_TABLE}, ambos na mesma transação."
    )
    confirmed = input("Digite 'TRANSFERIR' para confirmar: ").strip()
    if confirmed != "TRANSFERIR":
        print("Cancelado pelo usuário.")
        return 1

    try:
        inserted, deleted = transfer(engine, EXCLUDED_IDS)
    except Exception as exc:
        print(f"FALHA: {exc}")
        return 3

    print(
        f"OK. {inserted} linha(s) inserida(s) em {PAID_COMMISSIONS_TABLE} e "
        f"{deleted} linha(s) excluída(s) de {EXCLUDED_COMMISSIONS_TABLE}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
