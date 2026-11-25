"""Confere a apuração de consórcio contra a planilha manual de agosto/2026.

Uso:
    python scripts/conferir_consorcio_agosto.py --pasta "<caminho da pasta Relatorios>"

O script não grava nada: apenas lê os CSV, aplica as regras e compara os
totais com os da planilha `Relatorio Cotas Consórcio Agosto 26.xlsx`.
"""

from __future__ import annotations

import argparse
import sys
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

from commission_tool.core.consorcio import build_premiacao_tables, enrich_consorcio_rows
from commission_tool.data.sources.postgres import read_manager_relations
from commission_tool.data.sources.postgres_consorcio import (
    read_consorcio_coordenador,
    read_consorcio_pdv_map,
    read_consorcio_regras,
    read_consorcio_vendedor_map,
)
from commission_tool.io.consorcio_csv import read_consorcio_csv

ESPERADO = {"cen": 112560.00, "gestor": 8456.564865, "coordenador": 6040.403475}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pasta", required=True, help="Pasta com os CSV do mês")
    args = parser.parse_args()

    arquivos = sorted(Path(args.pasta).glob("*.csv"))
    if not arquivos:
        raise SystemExit(f"Nenhum CSV encontrado em {args.pasta}")

    frames = []
    for arquivo in arquivos:
        with arquivo.open("rb") as handle:
            frames.append(read_consorcio_csv(handle, file_name=arquivo.name))
        print(f"{arquivo.name}: {len(frames[-1])} linhas")

    rows = enrich_consorcio_rows(
        pd.concat(frames, ignore_index=True),
        pdv_map=read_consorcio_pdv_map(),
        vendedor_map=read_consorcio_vendedor_map(),
        manager_relations=read_manager_relations(),
        regras=read_consorcio_regras(),
        coordenador=read_consorcio_coordenador(),
        paid_keys=None,
    )

    bloqueadas = rows[~rows["apto"]]
    if not bloqueadas.empty:
        print("\nLinhas não aptas por status:")
        print(bloqueadas["status"].value_counts().to_string())

    tabelas = build_premiacao_tables(rows)
    totais = {chave: float(tabela["Valor a pagar"].sum()) for chave, tabela in tabelas.items()}

    print("\nTotais apurados x planilha:")
    for chave, esperado in ESPERADO.items():
        apurado = totais.get(chave, 0.0)
        marca = "OK" if abs(apurado - esperado) < 0.01 else "DIVERGENTE"
        print(f"  {chave:12s} apurado={apurado:14,.2f}  planilha={esperado:14,.2f}  {marca}")


if __name__ == "__main__":
    main()
