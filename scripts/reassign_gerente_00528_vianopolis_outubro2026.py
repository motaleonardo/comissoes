"""One-off data fix: vendas do CEN 00528 na Loja Vianopolis vao para o gerente Clayton.

Regra de negocio:
    Em Outubro/2026 o vendedor LUCAS RODRIGUES DE OLIVEIRA (00528) vendeu nas
    filiais Loja Acreuna e Loja Vianopolis. As vendas de Vianopolis devem entrar
    nas comissoes do gerente Clayton Monteiro de Castro Filho, e nao do Reginaldo
    Pereira Garcia. Acreuna continua com o Reginaldo. O percentual (33%) e os
    valores nao mudam; troca apenas o nome do gerente.

Preview (padrao, nao altera nada):
    python scripts/reassign_gerente_00528_vianopolis_outubro2026.py

Aplicar:
    python scripts/reassign_gerente_00528_vianopolis_outubro2026.py --apply

Escopo: somente linhas de comissoespagas com cod_vendedor 00528, filial
Loja Vianopolis e mes_ano_comissao Outubro/2026.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env", encoding="utf-8")
except ImportError:
    pass

import pandas as pd
from sqlalchemy import text

from commission_tool.data.sources.postgres import (
    PAID_COMMISSIONS_TABLE,
    ensure_commission_tables,
    get_engine,
)

COD_VENDEDOR = "00528"
FILIAL = "LOJA VIANOPOLIS"
MES_ANO_COMISSAO = "Outubro/2026"
GERENTE_DESTINO = "Clayton Monteiro de Castro Filho"
MAX_ROWS_DEFAULT = 4
BACKUP_DIR = _PROJECT_ROOT / "backups"

_WHERE = """
    UPPER(TRIM(COALESCE(cod_vendedor, ''))) = :cod
    AND UPPER(TRIM(COALESCE(filial, ''))) = :filial
    AND mes_ano_comissao = :mes
"""
_PARAMS = {"cod": COD_VENDEDOR, "filial": FILIAL, "mes": MES_ANO_COMISSAO}


def _select(conn) -> pd.DataFrame:
    return pd.read_sql(
        text(
            f"""
            SELECT id, mes_ano_comissao, tipo, filial, nro_documento, nro_chassi, modelo,
                   cen, cod_vendedor, gerente, perc_comissao_gerente,
                   valor_comissao_total, valor_comissao_gerente
            FROM {PAID_COMMISSIONS_TABLE}
            WHERE {_WHERE}
            ORDER BY nro_documento, id
            """
        ),
        conn,
        params=_PARAMS,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="executa o UPDATE")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=MAX_ROWS_DEFAULT,
        help=f"aborta --apply se exceder este numero de linhas (default: {MAX_ROWS_DEFAULT})",
    )
    args = parser.parse_args()

    ensure_commission_tables()
    engine = get_engine()

    with engine.connect() as conn:
        before = _select(conn)

    print("=== PREVIEW: linhas que mudarao de gerente ===")
    if before.empty:
        print("Nenhuma linha encontrada.")
        return 0

    with pd.option_context("display.max_columns", None, "display.width", 220):
        print(before.to_string(index=False))
    print(f"\nTotal de linhas afetadas: {len(before)}")
    print(f"Comissao de gerente transferida: R$ {before['valor_comissao_gerente'].sum():,.2f}")

    if not args.apply:
        print("\nModo DRY-RUN. Nada alterado. Re-execute com --apply para confirmar.")
        return 0

    if len(before) > args.max_rows:
        print(f"\nABORTADO: {len(before)} linhas excedem o limite de {args.max_rows} (--max-rows).")
        return 2

    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"comissoespagas_gerente_{COD_VENDEDOR}_vianopolis_{stamp}.csv"
    before.to_csv(backup_path, index=False, encoding="utf-8-sig")
    print(f"\nBackup do estado anterior: {backup_path}")

    with engine.begin() as conn:
        result = conn.execute(
            text(f"UPDATE {PAID_COMMISSIONS_TABLE} SET gerente = :gerente WHERE {_WHERE}"),
            {**_PARAMS, "gerente": GERENTE_DESTINO},
        )
        updated = result.rowcount or 0

    print(f"OK. {updated} linha(s) atualizada(s) em {PAID_COMMISSIONS_TABLE}.")

    with engine.connect() as conn:
        after = _select(conn)
    print("\nEstado final:")
    with pd.option_context("display.max_columns", None, "display.width", 220):
        print(after.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
