"""Excel import/export helpers."""

from __future__ import annotations

import io

import pandas as pd

from commission_tool.core.eligibility import normalize_document_padded


def load_commission_spreadsheet(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file, sheet_name="Analitico CEN", header=23)
    df = df[df["Filial"].notna() & df["Filial"].astype(str).str.strip().ne("")]
    df = df[~df["Filial"].astype(str).str.strip().isin(["nan"])]
    df = df.reset_index(drop=True)

    df["Nro Documento"] = df["Nro Documento"].apply(normalize_document_padded)
    df["Nro Chassi"] = df["Nro Chassi"].fillna("").astype(str).str.strip()
    return df


def dataframe_to_excel_download(
    df: pd.DataFrame,
    sheet_name: str = "Resultado Validação",
) -> io.BytesIO:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max((len(str(cell.value or "")) for cell in col_cells), default=10)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
    buf.seek(0)
    return buf

