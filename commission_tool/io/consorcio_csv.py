"""Reader for the consortium system CSV reports."""

from __future__ import annotations

import pandas as pd

from commission_tool.core.formatting import parse_br_number


TEXT_COLUMNS = {
    "CD_Pedido": "cd_pedido",
    "NM_Produto": "nm_produto",
    "CD_Comissionado_PDV": "cd_pdv",
    "NM_Comissionado_PDV": "nm_pdv",
    "Consorciado": "consorciado",
    "CD_Grupo": "cd_grupo",
    "CD_Cota": "cd_cota",
    "Versao": "versao",
    "ST_Contemplacao": "st_contemplacao",
    "CD_Periodo": "cd_periodo",
    "DT_Alocacao": "dt_alocacao",
    "DT_Aprovacao": "dt_aprovacao",
    "Parcela": "parcela",
}
NUMERIC_COLUMNS = {
    "VL_Bem": "vl_bem",
    "PE_Comissao": "pe_comissao",
    "VL_Comissao": "vl_comissao",
}
VENDEDOR_CODE_COLUMNS = [
    "CD_Comissionado_Vendedor_Atual",
    "CD_Comissionado_Vendedor_Original",
]
VENDEDOR_NAME_COLUMNS = [
    "NM_Comissionado_Vendedor_Atual",
    "NM_Comissionado_Vendedor_Original",
]


class ConsorcioFileError(ValueError):
    """Raised when the uploaded file does not match the expected layout."""

    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__("Colunas obrigatórias ausentes: " + ", ".join(missing))


def _first_present(columns: list[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def read_consorcio_csv(uploaded_file, file_name: str = "") -> pd.DataFrame:
    """Read one consortium CSV into a normalized DataFrame."""
    raw = pd.read_csv(
        uploaded_file,
        sep=";",
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
    )
    raw.columns = [str(column).strip() for column in raw.columns]
    columns = list(raw.columns)

    missing = [column for column in {**TEXT_COLUMNS, **NUMERIC_COLUMNS} if column not in columns]
    code_column = _first_present(columns, VENDEDOR_CODE_COLUMNS)
    name_column = _first_present(columns, VENDEDOR_NAME_COLUMNS)
    if code_column is None:
        missing.append(VENDEDOR_CODE_COLUMNS[0])
    if name_column is None:
        missing.append(VENDEDOR_NAME_COLUMNS[0])
    if missing:
        raise ConsorcioFileError(sorted(missing))

    data = {target: raw[source].astype(str).str.strip() for source, target in TEXT_COLUMNS.items()}
    data["cd_vendedor_consorcio"] = raw[code_column].astype(str).str.strip()
    data["nm_vendedor_consorcio"] = raw[name_column].astype(str).str.strip()
    for source, target in NUMERIC_COLUMNS.items():
        data[target] = raw[source].apply(parse_br_number)

    prepared = pd.DataFrame(data)
    prepared.insert(0, "arquivo_origem", str(file_name or ""))
    return prepared.reset_index(drop=True)
