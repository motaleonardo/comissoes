"""One-off data fix: reassign CEN/cod_vendedor for specific documents in comissoespagas.

Run preview (default — does NOT change anything):
    python scripts/reassign_cen_for_documents.py

Apply the change:
    python scripts/reassign_cen_for_documents.py --apply

The script targets a hard-coded list of (nro_documento, new_cen, new_cod_vendedor)
tuples. Edit ASSIGNMENTS below before running for a different scenario.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

# Add project root to sys.path so `commission_tool` is importable
# when this script is run directly (e.g. `python scripts/reassign_cen_for_documents.py`).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Load .env so POSTGRES_URL / POSTGRES_* are available, mirroring the Streamlit app.
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


# Preserve the exact strings the operator requested (including trailing spaces in `cen`).
# NOTE: previous runs já processaram 000015825 e 000015826 — mantemos apenas o que
# está pendente para evitar re-execução desnecessária.
ASSIGNMENTS: list[dict[str, str]] = [
    {
        "nro_documento": "000015960",
        "cen": "CAIO FABIO DE SOUZA CARDOSO             ",
        "cod_vendedor": "AAETCP",
    },
]


# Matching estratégia:
# - Compara ignorando zeros à esquerda e espaços, para tolerar variações de formato
#   entre gravação via Streamlit (raw da view) e via upload Excel (zfill(9)).
#   Ex.: '000015826', '15826', '  000015826  ' → todos batem.
_MATCH_CLAUSE = "LTRIM(TRIM(COALESCE(nro_documento, '')), '0') = LTRIM(:doc, '0')"


def _normalized_doc(value: str) -> str:
    return value.strip().lstrip("0")


def preview(engine, assignments: Iterable[dict[str, str]]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for assignment in assignments:
        query = text(
            f"""
            SELECT
                id,
                nro_documento AS nro_documento_armazenado,
                nro_chassi,
                mes_ano_comissao,
                tipo,
                cen           AS cen_atual,
                cod_vendedor  AS cod_vendedor_atual,
                valor_comissao_total
            FROM {PAID_COMMISSIONS_TABLE}
            WHERE {_MATCH_CLAUSE}
            ORDER BY paid_at, id
            """
        )
        df = pd.read_sql(query, engine, params={"doc": _normalized_doc(assignment["nro_documento"])})
        df.insert(0, "nro_documento_alvo", assignment["nro_documento"])
        df["cen_novo"] = assignment["cen"]
        df["cod_vendedor_novo"] = assignment["cod_vendedor"]
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def apply(engine, assignments: Iterable[dict[str, str]]) -> int:
    total = 0
    with engine.begin() as conn:
        for assignment in assignments:
            result = conn.execute(
                text(
                    f"""
                    UPDATE {PAID_COMMISSIONS_TABLE}
                    SET cen = :cen,
                        cod_vendedor = :cod_vendedor
                    WHERE {_MATCH_CLAUSE}
                    """
                ),
                {
                    "doc": _normalized_doc(assignment["nro_documento"]),
                    "cen": assignment["cen"],
                    "cod_vendedor": assignment["cod_vendedor"],
                },
            )
            total += result.rowcount or 0
    return total


MAX_ROWS_DEFAULT = 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the UPDATE. Without this flag, only the preview is shown.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=MAX_ROWS_DEFAULT,
        help=(
            "Aborta --apply se o número de linhas afetadas exceder este limite "
            f"(default: {MAX_ROWS_DEFAULT}). Salvaguarda contra updates acidentais em massa."
        ),
    )
    args = parser.parse_args()

    ensure_commission_tables()
    engine = get_engine()

    print("=== PREVIEW: linhas que serão afetadas ===")
    df_preview = preview(engine, ASSIGNMENTS)
    if df_preview.empty:
        print("Nenhuma linha encontrada para os documentos informados.")
        return 0

    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df_preview.to_string(index=False))

    affected = len(df_preview)
    print()
    print(f"Total de linhas afetadas: {affected}")

    if not args.apply:
        print()
        print("Modo DRY-RUN. Nenhuma alteração feita. Re-execute com --apply para confirmar.")
        return 0

    if affected > args.max_rows:
        print()
        print(
            f"ABORTADO: {affected} linhas seriam atualizadas, mas o limite é {args.max_rows} "
            f"(--max-rows). Revise os documentos ou aumente o limite explicitamente se for proposital."
        )
        return 2

    print()
    confirmed = input("Digite 'APLICAR' para confirmar a atualização: ").strip()
    if confirmed != "APLICAR":
        print("Cancelado pelo usuário.")
        return 1

    updated = apply(engine, ASSIGNMENTS)
    print(f"OK. {updated} linha(s) atualizada(s) em {PAID_COMMISSIONS_TABLE}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
