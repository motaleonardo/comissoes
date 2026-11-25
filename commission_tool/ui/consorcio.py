"""Streamlit views for the consortium commission flow."""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from commission_tool.core.consorcio import (
    STATUS_APTO,
    build_premiacao_tables,
    enrich_consorcio_rows,
    suggest_cod_vendedor,
)
from commission_tool.core.periods import CommissionPeriod, build_period_options, default_base_period
from commission_tool.data.sources.postgres import read_manager_relations
from commission_tool.data.sources.postgres_consorcio import (
    ensure_consorcio_tables,
    read_consorcio_coordenador,
    read_consorcio_paid_keys,
    read_consorcio_pdv_map,
    read_consorcio_regras,
    read_consorcio_vendedor_map,
    register_missing_consorcio_vendedores,
    replace_active_consorcio_pdv_map,
    replace_active_consorcio_regras,
    replace_active_consorcio_vendedor_map,
    save_consorcio_coordenador,
    save_consorcio_paid,
)
from commission_tool.io.consorcio_csv import ConsorcioFileError, read_consorcio_csv
from commission_tool.io.consorcio_excel import build_consorcio_workbook


PDV_EDITOR_COLUMNS = ["cd_pdv", "nm_pdv", "filial"]
VENDEDOR_EDITOR_COLUMNS = [
    "cd_vendedor_consorcio",
    "nm_vendedor_consorcio",
    "cod_vendedor",
    "sugestao_cod_vendedor",
    "sugestao_cen",
]


def consorcio_period_label(period: CommissionPeriod) -> str:
    """Return the commission period label used to save and read consortium data.

    This must always match ``build_commission_period(...).label`` (e.g. "Agosto/2026"),
    the same format used by every other report and email in the app. Never build a
    synthetic label such as "08/2026" here — it would be written to
    ``comissoes_consorcio_pagas.mes_ano_comissao`` but never matched by the reports
    or e-mail plan that read it back by ``period_label``.
    """
    return period.label


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
    vendedor_file = st.file_uploader(
        "Excel com as colunas CD_Comissionado_Vendedor, NM_Comissionado_Vendedor e Cod Vendedor",
        type=["xlsx"],
        key="consorcio_vendedor_upload",
    )
    if vendedor_file is not None and st.button(
        "Salvar de-para de Vendedor x CEN",
        key="consorcio_vendedor_save_upload",
    ):
        count = replace_active_consorcio_vendedor_map(pd.read_excel(vendedor_file))
        st.success(f"{count} vendedores salvos.")

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


def render_consorcio_apuracao() -> None:
    st.markdown("## Apuração de Consórcio")
    ensure_consorcio_tables()

    hoje = date.today()
    periods = build_period_options(hoje, years_back=1, years_ahead=1)
    labels = [period.label for period in periods]
    default_label = default_base_period(hoje).label
    default_index = labels.index(default_label) if default_label in labels else 0
    selected_label = st.selectbox("Competência", options=labels, index=default_index, key="consorcio_periodo")
    selected_period = periods[labels.index(selected_label)]
    period_label = consorcio_period_label(selected_period)
    ano = selected_period.base_year
    mes = selected_period.base_month

    uploads = st.file_uploader(
        "Relatórios do sistema de consórcio (um CSV por loja)",
        type=["csv"],
        accept_multiple_files=True,
        key="consorcio_uploads",
    )
    if not uploads:
        st.info("Envie os arquivos para iniciar a apuração.")
        return

    frames = []
    for uploaded in uploads:
        try:
            frames.append(read_consorcio_csv(uploaded, file_name=uploaded.name))
            st.success(f"{uploaded.name}: {len(frames[-1])} linhas lidas.")
        except ConsorcioFileError as exc:
            st.error(f"{uploaded.name}: {exc}")
    if not frames:
        return

    raw = pd.concat(frames, ignore_index=True)
    novos = register_missing_consorcio_vendedores(raw)
    if novos:
        st.warning(
            f"{novos} vendedor(es) de consórcio novo(s) cadastrado(s) sem CEN. "
            "Preencha em Configurações para liberar o pagamento."
        )

    rows = enrich_consorcio_rows(
        raw,
        pdv_map=read_consorcio_pdv_map(),
        vendedor_map=read_consorcio_vendedor_map(),
        manager_relations=read_manager_relations(),
        regras=read_consorcio_regras(),
        coordenador=read_consorcio_coordenador(),
        paid_keys=read_consorcio_paid_keys(),
    )

    aptos = rows[rows["apto"]]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Linhas", len(rows))
    col2.metric("CEN", f"R$ {aptos['valor_comissao_cen'].sum():,.2f}")
    col3.metric("Gestores", f"R$ {aptos['valor_comissao_gestor'].sum():,.2f}")
    col4.metric("Coordenador", f"R$ {aptos['valor_comissao_coordenador'].sum():,.2f}")

    bloqueadas = rows[~rows["apto"] & rows["status"].ne("Sem comissão")]
    if not bloqueadas.empty:
        with st.expander(f"{len(bloqueadas)} linha(s) bloqueada(s)", expanded=True):
            st.dataframe(bloqueadas["status"].value_counts().rename("Linhas"), width="stretch")

    editor_df = rows.copy()
    editor_df.insert(0, "Selecionar", editor_df["apto"])
    edited = st.data_editor(
        editor_df,
        width="stretch",
        disabled=[column for column in editor_df.columns if column != "Selecionar"],
        key="consorcio_apuracao_editor",
    )
    selecionadas = edited[edited["Selecionar"].fillna(False) & edited["apto"]].drop(columns=["Selecionar"])

    premiacao = build_premiacao_tables(selecionadas)
    st.markdown("### Premiação a pagar")
    st.dataframe(premiacao["cen"], width="stretch")
    st.dataframe(premiacao["gestor"], width="stretch")
    st.dataframe(premiacao["coordenador"], width="stretch")

    st.download_button(
        "Exportar Excel",
        data=build_consorcio_workbook(selecionadas, premiacao),
        file_name=f"consorcio_{int(ano)}_{int(mes):02d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )

    confirmar = st.checkbox(
        f"Confirmo o pagamento de {len(selecionadas)} linha(s) na competência {period_label}",
        key="consorcio_confirmar",
    )
    if st.button("Registrar pagamento", type="primary", disabled=not confirmar, width="stretch"):
        gravadas = save_consorcio_paid(selecionadas, int(ano), int(mes), period_label)
        st.success(f"{gravadas} linha(s) gravada(s) em {period_label}.")
