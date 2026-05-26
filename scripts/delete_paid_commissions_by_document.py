"""One-off data fix: hard delete rows in comissoespagas by nro_documento.

Run preview (default — does NOT change anything):
    python scripts/delete_paid_commissions_by_document.py

Apply the deletion:
    python scripts/delete_paid_commissions_by_document.py --apply

The script targets a hard-coded list of nro_documento values. Edit DOCUMENTS
below before running for a different scenario.

Safety: --apply aborts when the preview yields more than --max-rows (default 4)
to prevent accidental mass deletions.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

# Add project root to sys.path so `commission_tool` is importable
# when this script is run directly (e.g. `python scripts/delete_paid_commissions_by_document.py`).
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
    PAID_COMMISSIONS_TABLE,
    ensure_commission_tables,
    get_engine,
)


# Edit this list to delete different documents.
DOCUMENTS: list[str] = [
    "000006403",
    "000006420",
]


# Match estratégia: ignora zeros à esquerda e espaços para tolerar variações de
# formato entre gravação via Streamlit e via upload de Excel histórico.
_MATCH_CLAUSE = "LTRIM(TRIM(COALESCE(nro_documento, '')), '0') = LTRIM(:doc, '0')"


def _normalized_doc(value: str) -> str:
    return value.strip().lstrip("0")


def preview(engine, documents: Iterable[str]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for doc in documents:
        query = text(
            f"""
            SELECT
                id,
                nro_documento AS nro_documento_armazenado,
                nro_chassi,
                mes_ano_comissao,
                tipo,
                cen,
                cod_vendedor,
                gerente,
                classificacao_venda,
                valor_comissao_total,
                fonte,
                paid_at
            FROM {PAID_COMMISSIONS_TABLE}
            WHERE {_MATCH_CLAUSE}
            ORDER BY paid_at, id
            """
        )
        df = pd.read_sql(query, engine, params={"doc": _normalized_doc(doc)})
        df.insert(0, "nro_documento_alvo", doc)
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def apply(engine, documents: Iterable[str]) -> int:
    total = 0
    with engine.begin() as conn:
        for doc in documents:
            result = conn.execute(
                text(
                    f"""
                    DELETE FROM {PAID_COMMISSIONS_TABLE}
                    WHERE {_MATCH_CLAUSE}
                    """
                ),
                {"doc": _normalized_doc(doc)},
            )
            total += result.rowcount or 0
    return total


MAX_ROWS_DEFAULT = 4  # 2 documentos × até 2 linhas cada


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the DELETE. Without this flag, only the preview is shown.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=MAX_ROWS_DEFAULT,
        help=(
            "Aborta --apply se o número de linhas afetadas exceder este limite "
            f"(default: {MAX_ROWS_DEFAULT}). Salvaguarda contra exclusões em massa."
        ),
    )
    args = parser.parse_args()

    ensure_commission_tables()
    engine = get_engine()

    print("=== PREVIEW: linhas que serão EXCLUÍDAS ===")
    df_preview = preview(engine, DOCUMENTS)
    if df_preview.empty:
        print("Nenhuma linha encontrada para os documentos informados.")
        return 0

    with pd.option_context("display.max_columns", None, "display.width", 220):
        print(df_preview.to_string(index=False))

    affected = len(df_preview)
    print()
    print(f"Total de linhas a EXCLUIR: {affected}")

    if not args.apply:
        print()
        print("Modo DRY-RUN. Nenhuma alteração feita. Re-execute com --apply para confirmar.")
        return 0

    if affected > args.max_rows:
        print()
        print(
            f"ABORTADO: {affected} linhas seriam excluídas, mas o limite é {args.max_rows} "
            f"(--max-rows). Revise os documentos ou aumente o limite explicitamente se for proposital."
        )
        return 2

    print()
    print("⚠️  ATENÇÃO: esta operação é IRREVERSÍVEL.")
    confirmed = input("Digite 'EXCLUIR' para confirmar a deleção definitiva: ").strip()
    if confirmed != "EXCLUIR":
        print("Cancelado pelo usuário.")
        return 1

    deleted = apply(engine, DOCUMENTS)
    print(f"OK. {deleted} linha(s) excluída(s) de {PAID_COMMISSIONS_TABLE}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
