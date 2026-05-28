"""Excel import/export helpers."""

from __future__ import annotations

import io
import re

import pandas as pd

from commission_tool.core.eligibility import normalize_document_padded


EXCEL_SHEET_NAME_MAX = 31
EXCEL_INVALID_SHEET_CHARS = re.compile(r"[\\/?*\[\]:]")


def sanitize_sheet_name(name: str, used: set[str]) -> str:
    """Return a unique, Excel-safe sheet name (<=31 chars, no forbidden chars)."""
    base = EXCEL_INVALID_SHEET_CHARS.sub("_", str(name or "").strip()) or "Chassi"
    base = base[:EXCEL_SHEET_NAME_MAX]
    candidate = base
    counter = 2
    while candidate.upper() in {existing.upper() for existing in used}:
        suffix = f"_{counter}"
        candidate = f"{base[: EXCEL_SHEET_NAME_MAX - len(suffix)]}{suffix}"
        counter += 1
    used.add(candidate)
    return candidate


def load_commission_spreadsheet(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file, sheet_name="Analitico CEN", header=23)
    df = df[df["Filial"].notna() & df["Filial"].astype(str).str.strip().ne("")]
    df = df[~df["Filial"].astype(str).str.strip().isin(["nan"])]
    df = df.reset_index(drop=True)

    df["Nro Documento"] = df["Nro Documento"].apply(normalize_document_padded)
    df["Nro Chassi"] = df["Nro Chassi"].fillna("").astype(str).str.strip()
    return df


def _strip_datetime_tz(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.tz_localize(None)
    return out


def _autosize_columns(worksheet) -> None:
    for col_cells in worksheet.columns:
        max_len = max((len(str(cell.value or "")) for cell in col_cells), default=10)
        worksheet.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)


def build_chassis_history_workbook(
    history_by_chassis: dict[str, pd.DataFrame],
    index_df: pd.DataFrame | None = None,
) -> io.BytesIO:
    """Build a multi-sheet workbook: 'Indice' first, then one sheet per chassis.

    `history_by_chassis` keys are chassis numbers (already filtered to those with history).
    `index_df` is the summary table shown on the first sheet (chassi, lançamentos, soma de comissão).
    """
    buf = io.BytesIO()
    used: set[str] = {"Indice"}
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if index_df is None:
            index_df = pd.DataFrame(columns=["Nro Chassi", "Lançamentos no Histórico", "Valor Comissão Total Histórico"])
        _strip_datetime_tz(index_df).to_excel(writer, index=False, sheet_name="Indice")
        _autosize_columns(writer.sheets["Indice"])

        for chassis, history in history_by_chassis.items():
            sheet = sanitize_sheet_name(chassis, used)
            frame = history if isinstance(history, pd.DataFrame) else pd.DataFrame(history)
            _strip_datetime_tz(frame).to_excel(writer, index=False, sheet_name=sheet)
            _autosize_columns(writer.sheets[sheet])

    buf.seek(0)
    return buf


def build_multi_sheet_workbook(sheets: list[tuple[str, pd.DataFrame]]) -> io.BytesIO:
    buf = io.BytesIO()
    used: set[str] = set()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if not sheets:
            sheets = [("Relatorio", pd.DataFrame())]
        for sheet_name, df in sheets:
            safe_sheet = sanitize_sheet_name(sheet_name, used)
            frame = df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
            _strip_datetime_tz(frame).to_excel(writer, index=False, sheet_name=safe_sheet)
            _autosize_columns(writer.sheets[safe_sheet])
    buf.seek(0)
    return buf


def dataframe_to_excel_download(
    df: pd.DataFrame,
    sheet_name: str = "Resultado Validação",
) -> io.BytesIO:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        _strip_datetime_tz(df).to_excel(writer, index=False, sheet_name=sheet_name)
        _autosize_columns(writer.sheets[sheet_name])
    buf.seek(0)
    return buf
