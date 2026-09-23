"""Excel export for the consortium commission payout."""

from __future__ import annotations

import io

import pandas as pd

from commission_tool.io.excel import _autosize_columns, _strip_datetime_tz


def build_consorcio_workbook(base_df: pd.DataFrame, premiacao: dict[str, pd.DataFrame]) -> io.BytesIO:
    """Build the workbook that mirrors the manual spreadsheet: Base + three payout tabs."""
    buffer = io.BytesIO()
    sheets = [
        ("Base", base_df),
        ("Premiação CEN", premiacao.get("cen")),
        ("Premiação Gestores", premiacao.get("gestor")),
        ("Premiação Coordenador", premiacao.get("coordenador")),
    ]
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, frame in sheets:
            data = _strip_datetime_tz(frame if frame is not None else pd.DataFrame())
            data.to_excel(writer, sheet_name=sheet_name, index=False)
            _autosize_columns(writer.sheets[sheet_name])
    buffer.seek(0)
    return buffer
