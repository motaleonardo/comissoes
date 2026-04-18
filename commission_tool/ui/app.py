"""Streamlit app for commission validation and future calculation flow."""

from __future__ import annotations

import os
import time
from datetime import date
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from commission_tool.config import DISPLAY_COLUMNS, STATUS_APTO, STATUS_NAO_APTO, STATUS_VERIFICAR
from commission_tool.core.eligibility import diagnose_key_formats, run_eligibility_validation
from commission_tool.core.periods import MONTH_NAMES_PT, build_period_options, default_base_period
from commission_tool.data.pipeline import extract_incentive_titles, extract_machine_commission_base
from commission_tool.data.sources.postgres import (
    ensure_commission_tables,
    read_paid_commissions,
    save_incentive_titles,
    save_model_fat_rates,
    save_model_margin_rates,
    save_paid_commissions,
)
from commission_tool.data.sources.sqlserver import get_connection
from commission_tool.io.excel import dataframe_to_excel_download, load_commission_spreadsheet


def render_header() -> None:
    st.markdown(
        """
        <style>
            .main-header {
                background: linear-gradient(135deg, #1a3a5c 0%, #0d6e3f 100%);
                padding: 1.5rem 2rem;
                border-radius: 12px;
                margin-bottom: 1.5rem;
                color: white;
            }
            .main-header h1 { margin: 0; font-size: 1.8rem; }
            .main-header p  { margin: 0.3rem 0 0; opacity: 0.85; font-size: 0.95rem; }
            .section-title {
                font-size: 1rem;
                font-weight: 700;
                color: #1a3a5c;
                border-bottom: 2px solid #1a3a5c;
                padding-bottom: 0.3rem;
                margin: 1.2rem 0 0.8rem;
            }
            div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
        </style>
        <div class="main-header">
            <h1>✅ Validador de Comissões</h1>
            <p>Grupo Luiz Hohl - Validação de Incentivos e Pagamento de Clientes</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> str:
    with st.sidebar:
        st.markdown("### ⚙️ Configuração do Banco")
        server = os.getenv("DB_SERVER", "")
        database = os.getenv("DB_NAME", "")

        if server:
            st.info(f"🖥️ Servidor: **{server}**")
        else:
            st.warning("⚠️ `DB_SERVER` não definido no `.env`")

        if database:
            st.info(f"🗄️ Banco: **{database}**")
        else:
            st.warning("⚠️ `DB_NAME` não definido no `.env`")

        use_windows = st.checkbox("Autenticação Windows (Trusted)", value=False)
        username = ""
        password = ""
        if not use_windows:
            username = st.text_input("Usuário SQL")
            password = st.text_input("Senha SQL", type="password")

        if st.button("🔌 Testar Conexão", use_container_width=True):
            if not server or not database:
                st.error("Configure `DB_SERVER` e `DB_NAME` no arquivo `.env`.")
            else:
                try:
                    conn_test = get_connection(server, database, username, password, use_windows)
                    conn_test.close()
                    st.session_state.conn_ok = True
                    st.session_state.conn_cfg = {
                        "server": server,
                        "database": database,
                        "username": username,
                        "password": password,
                        "use_windows_auth": use_windows,
                    }
                    st.success("✅ Conexão estabelecida!")
                except Exception as exc:
                    st.session_state.conn_ok = False
                    st.error(f"❌ Falha: {exc}")

        st.divider()
        st.markdown("**Status da conexão:**")
        if st.session_state.conn_ok:
            st.success("🟢 Conectado")
        else:
            st.warning("🔴 Não conectado")

        st.divider()
        st.markdown(
            """
            **Tabelas esperadas no BD:**
            - `bdnIncentivos`
            - `bdnFaturamento`
            - `bdnContasReceber`
            """
        )

        st.divider()
        st.markdown("### Processo")
        return st.radio(
            "Selecione a visão",
            options=["Apurações", "Comissões pagas", "Relatórios", "Configurações"],
            label_visibility="collapsed",
        )


def render_paid_commissions_view() -> None:
    st.markdown('<div class="section-title">Comissões pagas</div>', unsafe_allow_html=True)
    st.caption("Consulta do histórico pago e importação inicial de comissões pagas anteriormente.")
    today = date.today()
    col_month, col_year, col_schema = st.columns([2, 1, 1])
    with col_month:
        paid_month = st.selectbox(
            "Mês",
            options=list(MONTH_NAMES_PT.keys()),
            format_func=lambda month: MONTH_NAMES_PT[month],
            index=max(0, today.month - 2),
            key="paid_lookup_month",
        )
    with col_year:
        paid_year = st.number_input(
            "Ano",
            min_value=2020,
            max_value=today.year + 5,
            value=today.year,
            step=1,
            key="paid_lookup_year",
        )
    with col_schema:
        st.write("")
        st.write("")
        if st.button("Garantir tabelas", use_container_width=True):
            try:
                ensure_commission_tables()
                st.success("Tabelas Postgres prontas.")
            except Exception as exc:
                st.error(f"Não foi possível preparar as tabelas: {exc}")

    if st.button("Consultar pagas", use_container_width=True):
        try:
            df_paid = read_paid_commissions(int(paid_year), int(paid_month))
            st.session_state.paid_commissions_lookup = df_paid
            st.success(f"{len(df_paid)} registros encontrados.")
        except Exception as exc:
            st.warning(f"Consulta indisponível: {exc}")

    df_paid = st.session_state.get("paid_commissions_lookup")
    if df_paid is not None and not df_paid.empty:
        total_paid = pd.to_numeric(df_paid.get("valor_comissao_total"), errors="coerce").sum()
        st.metric("Total pago", f"R$ {total_paid:,.2f}")
        sidebar_cols = [
            "mes_ano_comissao",
            "filial",
            "nro_documento",
            "nro_chassi",
            "modelo",
            "cen",
            "valor_comissao_total",
        ]
        sidebar_cols = [col for col in sidebar_cols if col in df_paid.columns]
        st.dataframe(df_paid[sidebar_cols], use_container_width=True, height=420)

        buf_paid = dataframe_to_excel_download(df_paid, sheet_name="Comissões Pagas")
        st.download_button(
            "Exportar consulta",
            data=buf_paid,
            file_name=f"comissoes_pagas_{int(paid_year)}_{int(paid_month):02d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.divider()
    st.markdown('<div class="section-title">Importar histórico</div>', unsafe_allow_html=True)
    periods = build_period_options(today, years_back=3, years_ahead=1)
    period_labels = [item.label for item in periods]
    default_label = default_base_period(today).label
    default_index = period_labels.index(default_label) if default_label in period_labels else 0
    selected_label = st.selectbox("Competência do arquivo", period_labels, index=default_index)
    selected_period = periods[period_labels.index(selected_label)]

    historical_file = st.file_uploader(
        "Arquivo Excel com comissões pagas",
        type=["xlsx", "xls"],
        key="historical_paid_commissions_upload",
    )
    if historical_file is not None:
        try:
            df_historical = pd.read_excel(historical_file)
            st.dataframe(df_historical.head(50), use_container_width=True, height=250)
            if st.button("Importar histórico para comissoespagas", use_container_width=True):
                saved_count = save_paid_commissions(
                    df_historical,
                    competence_year=selected_period.base_year,
                    competence_month=selected_period.base_month,
                    period_label=selected_period.label,
                    period_start=selected_period.start_date,
                    period_end=selected_period.end_date,
                    source="upload_excel",
                    file_name=historical_file.name,
                )
                st.success(f"{saved_count} linhas históricas importadas em comissoespagas.")
        except Exception as exc:
            st.error(f"Não foi possível importar o histórico: {exc}")


def render_upload_area():
    col_upload, col_info = st.columns([2, 1])

    with col_upload:
        st.markdown('<div class="section-title">📂 Carregar Planilha de Comissões</div>', unsafe_allow_html=True)
        uploaded = st.file_uploader(
            "Selecione o arquivo Excel (aba: Analitico CEN)",
            type=["xlsx"],
            label_visibility="collapsed",
        )

    with col_info:
        st.markdown('<div class="section-title">ℹ️ Regras aplicadas</div>', unsafe_allow_html=True)
        st.markdown(
            """
            **V1 - Incentivo:**
            - Chassi -> NF em `bdnIncentivos`
            - Soma saldo em `bdnContasReceber`
            - Saldo = 0 -> ✅ APTO

            **V2 - Pagamento Cliente:**
            - Documento -> cliente em `bdnFaturamento`
            - Soma saldo em `bdnContasReceber`
            - Saldo = 0 -> ✅ APTO
            - Saldo != 0 + tipo **BL** -> ✅ APTO
            - Saldo != 0 + outro tipo -> ❌ NÃO APTO
            - Data divergente -> ⚠️ VERIFICAR
            """
        )

    return uploaded


def render_spreadsheet_preview(df_planilha: pd.DataFrame) -> None:
    st.success(f"✅ Planilha carregada - **{len(df_planilha)} registros** encontrados na aba 'Analitico CEN'")

    col_a, col_b = st.columns(2)
    col_a.metric("Total de registros", len(df_planilha))
    col_b.metric(
        "Valor total comissão",
        f"R$ {pd.to_numeric(df_planilha['Valor Comissão Total'], errors='coerce').sum():,.2f}",
    )

    with st.expander("👁️ Prévia da planilha"):
        preview_cols = [
            "Filial",
            "CEN",
            "Cliente",
            "Nro Documento",
            "Nro Chassi",
            "Data de Emissão",
            "Classificação Venda",
            "% Comissão NF",
            "Valor Comissão NF",
            "Meta Margem",
            "% Margem Bruta",
            "Valor Comissão Margem",
            "Valor Comissão Total",
        ]
        preview_cols = [col for col in preview_cols if col in df_planilha.columns]
        st.dataframe(df_planilha[preview_cols], use_container_width=True, height=250)


def render_diagnostics(df_planilha: pd.DataFrame) -> None:
    with st.expander("🔍 Diagnóstico de Dados - verificar formatos antes de validar"):
        if st.button("▶️ Executar Diagnóstico", use_container_width=True):
            cfg = st.session_state.conn_cfg
            try:
                conn_diag = get_connection(**cfg)
                with st.spinner("Verificando formatos..."):
                    df_diag, resumo = diagnose_key_formats(conn_diag, df_planilha)
                conn_diag.close()

                cd1, cd2 = st.columns(2)
                taxa_chassi = (
                    resumo["chassi_ok"] / resumo["chassi_total"] * 100
                    if resumo["chassi_total"] > 0
                    else 0
                )
                taxa_doc = (
                    resumo["doc_ok"] / resumo["doc_total"] * 100
                    if resumo["doc_total"] > 0
                    else 0
                )
                cd1.metric(
                    "Chassi encontrados no BD",
                    f"{resumo['chassi_ok']}/{resumo['chassi_total']}",
                    f"{taxa_chassi:.0f}%",
                    delta_color="normal" if taxa_chassi >= 80 else "off",
                )
                cd2.metric(
                    "Documentos encontrados no BD",
                    f"{resumo['doc_ok']}/{resumo['doc_total']}",
                    f"{taxa_doc:.0f}%",
                    delta_color="normal" if taxa_doc >= 80 else "off",
                )

                if taxa_chassi < 80 or taxa_doc < 80:
                    st.warning("⚠️ Taxa de correspondência baixa - verifique os formatos dos dados abaixo.")
                else:
                    st.success("✅ Dados da planilha correspondem bem ao banco de dados.")

                st.dataframe(df_diag, use_container_width=True, height=300)
            except Exception as exc:
                st.error(f"Erro no diagnóstico: {exc}")


def run_validation(df_planilha: pd.DataFrame) -> None:
    if st.button("🚀 Executar Validação Completa", type="primary", use_container_width=True):
        cfg = st.session_state.conn_cfg
        try:
            conn = get_connection(**cfg)
            progress = st.progress(0)
            status = st.empty()

            def progress_callback(current, total, row):
                status.text(f"Validando linha {current} de {total} - {str(row.get('CEN', ''))[:40]}")
                progress.progress(current / total)

            t0 = time.time()
            with st.spinner("Validando comissões..."):
                results_df = run_eligibility_validation(conn, df_planilha, progress_callback)
            elapsed = time.time() - t0

            conn.close()
            progress.empty()
            status.empty()
            st.session_state.results_df = results_df

            minutes, seconds = divmod(int(elapsed), 60)
            st.success(f"✅ Validação concluída em {minutes}m {seconds}s!")
        except Exception as exc:
            st.error(f"Erro durante validação: {exc}")


def render_results() -> None:
    if st.session_state.results_df is None:
        return

    df_r = st.session_state.results_df
    if "V1 - Status" not in df_r.columns:
        st.session_state.results_df = None
        st.warning("Resultados anteriores foram limpos. Execute a validação novamente.")
        st.stop()

    st.markdown('<div class="section-title">📊 Resultados da Validação</div>', unsafe_allow_html=True)

    total = len(df_r)
    aptos = (df_r["Status Geral"] == STATUS_APTO).sum()
    n_aptos = (df_r["Status Geral"] == STATUS_NAO_APTO).sum()
    verif = (df_r["Status Geral"] == STATUS_VERIFICAR).sum()
    val_apta = pd.to_numeric(df_r.loc[df_r["Status Geral"] == STATUS_APTO, "Valor Comissão"], errors="coerce").sum()
    val_n_apta = pd.to_numeric(
        df_r.loc[df_r["Status Geral"] == STATUS_NAO_APTO, "Valor Comissão"],
        errors="coerce",
    ).sum()

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total", total)
    k2.metric("✅ Aptos", aptos)
    k3.metric("❌ Não Aptos", n_aptos)
    k4.metric("⚠️ Verificar", verif)
    k5.metric("Valor Apto", f"R$ {val_apta:,.2f}")
    k6.metric("Valor Bloqueado", f"R$ {val_n_apta:,.2f}")

    st.divider()
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        filtro_status = st.multiselect(
            "Filtrar por Status Geral",
            options=df_r["Status Geral"].unique().tolist(),
            default=df_r["Status Geral"].unique().tolist(),
        )
    with col_f2:
        filtro_filial = st.multiselect(
            "Filtrar por Filial",
            options=sorted(df_r["Filial"].dropna().unique().tolist()),
            default=[],
        )
    with col_f3:
        filtro_v1 = st.multiselect(
            "Status V1 (Incentivo)",
            options=df_r["V1 - Status"].unique().tolist(),
            default=[],
        )

    df_show = df_r[df_r["Status Geral"].isin(filtro_status)]
    if filtro_filial:
        df_show = df_show[df_show["Filial"].isin(filtro_filial)]
    if filtro_v1:
        df_show = df_show[df_show["V1 - Status"].isin(filtro_v1)]

    display_cols = [col for col in DISPLAY_COLUMNS if col in df_show.columns]
    st.dataframe(
        df_show[display_cols],
        use_container_width=True,
        height=450,
        column_config={
            "Status Geral": st.column_config.TextColumn("Status", width=120),
            "Valor Comissão": st.column_config.NumberColumn("Comissão R$", format="R$ %.2f"),
            "V1 - Saldo Incentivo": st.column_config.NumberColumn("V1 Saldo", format="R$ %.2f"),
            "V2 - Saldo Cliente": st.column_config.NumberColumn("V2 Saldo", format="R$ %.2f"),
        },
    )
    st.caption(f"Exibindo {len(df_show)} de {total} registros")

    st.divider()
    st.markdown('<div class="section-title">📥 Exportar Resultado</div>', unsafe_allow_html=True)
    col_exp1, col_exp2 = st.columns(2)
    with col_exp1:
        buf_all = dataframe_to_excel_download(df_r[display_cols])
        st.download_button(
            "⬇️ Exportar TODOS os resultados",
            data=buf_all,
            file_name=f"validacao_comissoes_{datetime.today().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with col_exp2:
        df_pendencias = df_r[df_r["Status Geral"].isin([STATUS_NAO_APTO, STATUS_VERIFICAR])]
        buf_pendencias = dataframe_to_excel_download(df_pendencias[display_cols])
        st.download_button(
            "⬇️ Exportar apenas NÃO APTOS / VERIFICAR",
            data=buf_pendencias,
            file_name=f"pendencias_comissoes_{datetime.today().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


def render_machine_extraction() -> None:
    st.markdown('<div class="section-title">🚜 Apuração de Máquinas</div>', unsafe_allow_html=True)
    st.caption("Extração unificada de faturamentos e devoluções no período 16-15.")

    today = date.today()
    periods = build_period_options(today, years_back=1, years_ahead=1)
    labels = [period.label for period in periods]
    default_label = default_base_period(today).label
    default_index = labels.index(default_label) if default_label in labels else 0

    col_period, col_action = st.columns([2, 1])
    with col_period:
        selected_label = st.selectbox(
            "Mês base da comissão",
            options=labels,
            index=default_index,
        )
    selected_period = periods[labels.index(selected_label)]

    st.info(
        "Período de apuração: "
        f"**{selected_period.start_date.strftime('%d/%m/%Y')}** até "
        f"**{selected_period.end_date.strftime('%d/%m/%Y')}**"
    )

    with col_action:
        st.write("")
        st.write("")
        extract_clicked = st.button("Extrair máquinas", type="primary", use_container_width=True)

    if not st.session_state.conn_ok:
        st.warning("Configure e teste a conexão com o banco de dados na barra lateral antes de extrair.")
        return

    if extract_clicked:
        cfg = st.session_state.conn_cfg
        try:
            conn = get_connection(**cfg)
            with st.spinner("Extraindo faturamentos, devoluções e enriquecimentos..."):
                df_machine = extract_machine_commission_base(
                    conn,
                    selected_period.start_date,
                    selected_period.end_date,
                )
                df_incentives = extract_incentive_titles(conn)
            conn.close()
            st.session_state.machine_df = df_machine
            st.session_state.incentive_titles_df = df_incentives
            st.session_state.machine_period_label = selected_period.label
            st.success(f"Extração concluída: {len(df_machine)} registros.")
        except Exception as exc:
            st.error(f"Erro durante extração de máquinas: {exc}")

    df_machine = st.session_state.get("machine_df")
    if df_machine is None:
        return

    st.markdown('<div class="section-title">Tabela Única de Máquinas</div>', unsafe_allow_html=True)

    total_receita = pd.to_numeric(df_machine["Receita Bruta"], errors="coerce").sum()
    total_margem = pd.to_numeric(df_machine["Margem R$"], errors="coerce").sum()
    total_incentivo = pd.to_numeric(df_machine["Valor Incentivo"], errors="coerce").sum()
    total_comissao = pd.to_numeric(df_machine["Valor Comissão Total"], errors="coerce").sum()

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Registros", len(df_machine))
    k2.metric("Receita Bruta", f"R$ {total_receita:,.2f}")
    k3.metric("Margem R$", f"R$ {total_margem:,.2f}")
    k4.metric("Incentivos", f"R$ {total_incentivo:,.2f}")
    k5.metric("Comissão Total", f"R$ {total_comissao:,.2f}")

    editable_df = df_machine.copy()
    if "Pagar" not in editable_df.columns:
        editable_df.insert(0, "Pagar", False)

    edited_df = st.data_editor(
        editable_df,
        use_container_width=True,
        height=450,
        disabled=[column for column in editable_df.columns if column != "Pagar"],
        column_config={
            "Pagar": st.column_config.CheckboxColumn("Pagar", default=False),
            "Receita Bruta": st.column_config.NumberColumn("Receita Bruta", format="R$ %.2f"),
            "Valor Comissão Fat.": st.column_config.NumberColumn("Comissão Fat.", format="R$ %.2f"),
            "CMV": st.column_config.NumberColumn("CMV", format="R$ %.2f"),
            "Margem R$": st.column_config.NumberColumn("Margem R$", format="R$ %.2f"),
            "% Margem Direta": st.column_config.NumberColumn("% Margem Direta", format="%.2f%%"),
            "Valor Incentivo": st.column_config.NumberColumn("Incentivos", format="R$ %.2f"),
            "Receita Bruta + Incentivos R$": st.column_config.NumberColumn(
                "Receita + Incentivos",
                format="R$ %.2f",
            ),
            "Margem + Incentivos R$": st.column_config.NumberColumn("Margem + Incentivos", format="R$ %.2f"),
            "% Margem Bruta": st.column_config.NumberColumn("% Margem Bruta", format="%.2f%%"),
            "Valor Comissão Margem": st.column_config.NumberColumn("Comissão Margem", format="R$ %.2f"),
            "Valor Comissão Total": st.column_config.NumberColumn("Comissão Total", format="R$ %.2f"),
        },
        key="machine_payment_editor",
    )
    selected_to_pay = edited_df[edited_df["Pagar"] == True].drop(columns=["Pagar"])
    st.caption(f"{len(selected_to_pay)} comissões selecionadas para pagamento.")

    col_pay, col_schema = st.columns(2)
    with col_pay:
        if st.button("Pagar comissões selecionadas", type="primary", use_container_width=True):
            if selected_to_pay.empty:
                st.warning("Marque ao menos uma comissão para pagar.")
            else:
                try:
                    saved_count = save_paid_commissions(
                        selected_to_pay,
                        competence_year=selected_period.base_year,
                        competence_month=selected_period.base_month,
                        period_label=selected_period.label,
                        period_start=selected_period.start_date,
                        period_end=selected_period.end_date,
                        source="streamlit",
                    )
                    st.success(f"{saved_count} comissões gravadas em comissoespagas.")
                except Exception as exc:
                    st.error(f"Não foi possível gravar as comissões pagas: {exc}")
    with col_schema:
        if st.button("Garantir tabelas Postgres", use_container_width=True):
            try:
                ensure_commission_tables()
                st.success("Tabelas Postgres prontas.")
            except Exception as exc:
                st.error(f"Não foi possível preparar as tabelas: {exc}")

    buf = dataframe_to_excel_download(df_machine, sheet_name="Apuração Máquinas")
    st.download_button(
        "⬇️ Exportar tabela única de máquinas",
        data=buf,
        file_name=f"apuracao_maquinas_{st.session_state.get('machine_period_label', 'periodo')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    df_incentives = st.session_state.get("incentive_titles_df")
    if df_incentives is None:
        return

    with st.expander("Títulos de incentivo por chassi"):
        st.caption("Tabela separada para auditoria dos títulos de incentivo e saldo no contas a receber.")
        st.dataframe(
            df_incentives,
            use_container_width=True,
            height=350,
            column_config={
                "Valor Incentivo": st.column_config.NumberColumn("Valor Incentivo", format="R$ %.2f"),
                "Saldo Incentivo": st.column_config.NumberColumn("Saldo Incentivo", format="R$ %.2f"),
            },
        )
        col_export, col_save = st.columns(2)
        with col_export:
            buf_incentives = dataframe_to_excel_download(df_incentives, sheet_name="Títulos Incentivo")
            st.download_button(
                "⬇️ Exportar títulos de incentivo",
                data=buf_incentives,
                file_name=f"titulos_incentivo_{st.session_state.get('machine_period_label', 'periodo')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with col_save:
            if st.button("Salvar títulos no Postgres", use_container_width=True):
                try:
                    save_incentive_titles(df_incentives)
                    st.success("Títulos de incentivo salvos no Postgres.")
                except Exception as exc:
                    st.error(f"Não foi possível salvar no Postgres: {exc}")
    return

    with st.expander("Upload de comissões pagas anteriormente"):
        historical_file = st.file_uploader(
            "Arquivo Excel com comissões pagas",
            type=["xlsx", "xls"],
            key="historical_paid_commissions_upload",
        )
        if historical_file is not None:
            try:
                df_historical = pd.read_excel(historical_file)
                st.dataframe(df_historical.head(50), use_container_width=True, height=250)
                if st.button("Importar histórico para comissoespagas", use_container_width=True):
                    saved_count = save_paid_commissions(
                        df_historical,
                        competence_year=selected_period.base_year,
                        competence_month=selected_period.base_month,
                        period_label=selected_period.label,
                        period_start=selected_period.start_date,
                        period_end=selected_period.end_date,
                        source="upload_excel",
                        file_name=historical_file.name,
                    )
                    st.success(f"{saved_count} linhas históricas importadas em comissoespagas.")
            except Exception as exc:
                st.error(f"Não foi possível importar o histórico: {exc}")

    with st.expander("Configurações de comissão por modelo"):
        st.caption("Uploads para alimentar as tabelas de percentuais por modelo.")
        fat_file = st.file_uploader(
            "Excel de % comissão sobre faturamento por Modelo",
            type=["xlsx", "xls"],
            key="fat_rate_upload",
        )
        if fat_file is not None:
            try:
                df_fat = pd.read_excel(fat_file)
                st.dataframe(df_fat.head(30), use_container_width=True, height=180)
                if st.button("Salvar % faturamento por modelo", use_container_width=True):
                    count = save_model_fat_rates(df_fat)
                    st.success(f"{count} modelos salvos em comissao_faturamento_modelo.")
            except Exception as exc:
                st.error(f"Não foi possível salvar percentuais de faturamento: {exc}")

        margin_file = st.file_uploader(
            "Excel de % comissão sobre margem por Modelo",
            type=["xlsx", "xls"],
            key="margin_rate_upload",
        )
        if margin_file is not None:
            try:
                df_margin = pd.read_excel(margin_file)
                st.dataframe(df_margin.head(30), use_container_width=True, height=180)
                if st.button("Salvar % margem por modelo", use_container_width=True):
                    count = save_model_margin_rates(df_margin)
                    st.success(f"{count} modelos salvos em comissao_margem_modelo.")
            except Exception as exc:
                st.error(f"Não foi possível salvar percentuais de margem: {exc}")


def render_spreadsheet_validation_flow() -> None:
    uploaded = render_upload_area()

    if uploaded:
        try:
            df_planilha = load_commission_spreadsheet(uploaded)
            render_spreadsheet_preview(df_planilha)
            st.divider()

            if not st.session_state.conn_ok:
                st.warning("⚠️ Configure e teste a conexão com o banco de dados na barra lateral antes de executar.")
            else:
                render_diagnostics(df_planilha)
                st.divider()
                run_validation(df_planilha)
        except Exception as exc:
            st.error(f"Erro ao ler planilha: {exc}")

    render_results()


def render_reports_view() -> None:
    st.markdown('<div class="section-title">Relatórios</div>', unsafe_allow_html=True)
    st.write("")


def render_settings_view() -> None:
    st.markdown('<div class="section-title">Configurações</div>', unsafe_allow_html=True)
    st.caption("Uploads e manutenção inicial dos percentuais de comissão por modelo.")

    col_schema, _ = st.columns([1, 2])
    with col_schema:
        if st.button("Garantir tabelas Postgres", use_container_width=True):
            try:
                ensure_commission_tables()
                st.success("Tabelas Postgres prontas.")
            except Exception as exc:
                st.error(f"Não foi possível preparar as tabelas: {exc}")

    st.divider()
    st.markdown("### Comissão sobre faturamento")
    fat_file = st.file_uploader(
        "Excel de % comissão sobre faturamento por Modelo",
        type=["xlsx", "xls"],
        key="settings_fat_rate_upload",
    )
    if fat_file is not None:
        try:
            df_fat = pd.read_excel(fat_file)
            st.dataframe(df_fat.head(50), use_container_width=True, height=250)
            if st.button("Salvar % faturamento por modelo", use_container_width=True):
                count = save_model_fat_rates(df_fat)
                st.success(f"{count} modelos salvos em comissao_faturamento_modelo.")
        except Exception as exc:
            st.error(f"Não foi possível salvar percentuais de faturamento: {exc}")

    st.divider()
    st.markdown("### Comissão sobre margem")
    margin_file = st.file_uploader(
        "Excel de % comissão sobre margem por Modelo",
        type=["xlsx", "xls"],
        key="settings_margin_rate_upload",
    )
    if margin_file is not None:
        try:
            df_margin = pd.read_excel(margin_file)
            st.dataframe(df_margin.head(50), use_container_width=True, height=250)
            if st.button("Salvar % margem por modelo", use_container_width=True):
                count = save_model_margin_rates(df_margin)
                st.success(f"{count} modelos salvos em comissao_margem_modelo.")
        except Exception as exc:
            st.error(f"Não foi possível salvar percentuais de margem: {exc}")


def main() -> None:
    load_dotenv()
    st.set_page_config(page_title="Validador de Comissões", page_icon="✅", layout="wide")

    if "conn_ok" not in st.session_state:
        st.session_state.conn_ok = False
    if "results_df" not in st.session_state:
        st.session_state.results_df = None
    if "machine_df" not in st.session_state:
        st.session_state.machine_df = None
    if "incentive_titles_df" not in st.session_state:
        st.session_state.incentive_titles_df = None
    if "paid_commissions_lookup" not in st.session_state:
        st.session_state.paid_commissions_lookup = None

    render_header()
    selected_view = render_sidebar()

    if selected_view == "Apurações":
        render_machine_extraction()
    elif selected_view == "Comissões pagas":
        render_paid_commissions_view()
    elif selected_view == "Relatórios":
        render_reports_view()
    elif selected_view == "Configurações":
        render_settings_view()
