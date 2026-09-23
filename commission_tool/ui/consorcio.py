"""Streamlit views for the consortium commission flow."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from commission_tool.core.consorcio import suggest_cod_vendedor
from commission_tool.data.sources.postgres import read_manager_relations
from commission_tool.data.sources.postgres_consorcio import (
    ensure_consorcio_tables,
    read_consorcio_coordenador,
    read_consorcio_pdv_map,
    read_consorcio_regras,
    read_consorcio_vendedor_map,
    replace_active_consorcio_pdv_map,
    replace_active_consorcio_regras,
    replace_active_consorcio_vendedor_map,
    save_consorcio_coordenador,
)


PDV_EDITOR_COLUMNS = ["cd_pdv", "nm_pdv", "filial"]
VENDEDOR_EDITOR_COLUMNS = [
    "cd_vendedor_consorcio",
    "nm_vendedor_consorcio",
    "cod_vendedor",
    "sugestao_cod_vendedor",
    "sugestao_cen",
]


def prepare_pdv_editor_df(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=PDV_EDITOR_COLUMNS)
    prepared = df.copy()
    for column in PDV_EDITOR_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = ""
    return prepared[PDV_EDITOR_COLUMNS].reset_index(drop=True)


def prepare_vendedor_editor_df(
    vendedor_map: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
) -> pd.DataFrame:
    prepared = suggest_cod_vendedor(vendedor_map, manager_relations)
    if prepared.empty:
        return pd.DataFrame(columns=VENDEDOR_EDITOR_COLUMNS)
    prepared["__pendente"] = prepared["cod_vendedor"].eq("")
    prepared = prepared.sort_values(
        ["__pendente", "nm_vendedor_consorcio"],
        ascending=[False, True],
    )
    return prepared[VENDEDOR_EDITOR_COLUMNS].reset_index(drop=True)


def render_consorcio_settings_sections() -> None:
    st.divider()
    st.markdown("### Consórcio: PDV x Filial")
    pdv_file = st.file_uploader(
        "Excel com as colunas CD_Comissionado_PDV, NM_Comissionado_PDV e Filial",
        type=["xlsx"],
        key="consorcio_pdv_upload",
    )
    if pdv_file is not None and st.button("Salvar de-para de PDV", key="consorcio_pdv_save_upload"):
        count = replace_active_consorcio_pdv_map(pd.read_excel(pdv_file))
        st.success(f"{count} PDVs salvos.")

    pdv_df = prepare_pdv_editor_df(read_consorcio_pdv_map())
    edited_pdv = st.data_editor(
        pdv_df,
        num_rows="dynamic",
        width="stretch",
        key="consorcio_pdv_editor",
    )
    if st.button("Salvar alterações de PDV", type="primary", key="consorcio_pdv_save_editor"):
        count = replace_active_consorcio_pdv_map(edited_pdv)
        st.success(f"{count} PDVs salvos.")

    st.divider()
    st.markdown("### Consórcio: Vendedor x CEN")
    st.caption(
        "Os vendedores novos entram automaticamente com o CEN em branco. "
        "A sugestão é apenas informativa: preencha o campo `cod_vendedor` para liberar o pagamento."
    )
    vendedor_df = prepare_vendedor_editor_df(read_consorcio_vendedor_map(), read_manager_relations())
    pendentes = int(vendedor_df["cod_vendedor"].eq("").sum()) if not vendedor_df.empty else 0
    if pendentes:
        st.warning(f"{pendentes} vendedor(es) de consórcio sem CEN definido.")
    edited_vendedor = st.data_editor(
        vendedor_df,
        num_rows="dynamic",
        width="stretch",
        disabled=["sugestao_cod_vendedor", "sugestao_cen"],
        key="consorcio_vendedor_editor",
    )
    if st.button("Salvar alterações de Vendedor x CEN", type="primary", key="consorcio_vendedor_save"):
        count = replace_active_consorcio_vendedor_map(edited_vendedor)
        st.success(f"{count} vendedores salvos.")

    st.divider()
    st.markdown("### Consórcio: regras por parcela")
    regras_df = read_consorcio_regras()
    edited_regras = st.data_editor(
        regras_df,
        num_rows="dynamic",
        width="stretch",
        key="consorcio_regras_editor",
    )
    if st.button("Salvar regras do consórcio", type="primary", key="consorcio_regras_save"):
        count = replace_active_consorcio_regras(edited_regras)
        st.success(f"{count} regras salvas.")

    st.divider()
    st.markdown("### Consórcio: coordenador")
    coordenador = read_consorcio_coordenador()
    nome = st.text_input("Nome do coordenador", value=coordenador.get("nome", ""), key="consorcio_coord_nome")
    email = st.text_input("E-mail do coordenador", value=coordenador.get("email", ""), key="consorcio_coord_email")
    if st.button("Salvar coordenador", type="primary", key="consorcio_coord_save"):
        save_consorcio_coordenador(nome, email)
        st.success("Coordenador salvo.")
