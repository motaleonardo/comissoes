"""Streamlit app for commission validation and future calculation flow."""

from __future__ import annotations

import os
import re
import time
from datetime import date
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from commission_tool.config import DISPLAY_COLUMNS, STATUS_APTO, STATUS_NAO_APTO, STATUS_VERIFICAR
from commission_tool.core.eligibility import diagnose_key_formats, run_eligibility_validation
from commission_tool.core.formatting import format_currency_br, format_percent_br
from commission_tool.core.paid_audit import (
    FAT_RATE_PERCENT_COLUMNS,
    MARGIN_RATE_PERCENT_COLUMNS,
    build_extraction_key_set,
    build_rate_lookup,
    normalize_commission_report_df,
    validate_paid_commission_file,
)
from commission_tool.core.periods import MONTH_NAMES_PT, build_period_options, default_base_period
from commission_tool.data.pipeline import (
    extract_incentive_titles,
    extract_machine_commission_base,
    extract_machine_incentive_audit,
    extract_machine_source_audit,
)
from commission_tool.data.sources.postgres import (
    append_model_fat_rates,
    append_model_margin_rates,
    ensure_commission_tables,
    get_paid_commissions_schema_status,
    read_paid_commissions,
    save_incentive_titles,
    save_model_fat_rates,
    save_model_margin_rates,
    save_paid_commissions,
)
from commission_tool.data.sources.sqlserver import get_connection
from commission_tool.io.excel import dataframe_to_excel_download, load_commission_spreadsheet


CURRENCY_COLUMNS = [
    "Receita Bruta",
    "Valor Comissão Fat.",
    "CMV",
    "Margem R$",
    "Valor Incentivo",
    "Receita Bruta + Incentivos R$",
    "Margem + Incentivos R$",
    "Valor Comissão Margem",
    "Valor Comissão Total",
    "Valor Comissão",
    "V1 - Saldo Incentivo",
    "V2 - Saldo Cliente",
    "Saldo Incentivo",
]

PERCENT_COLUMNS = [
    "% Comissão Fat.",
    "% Margem Direta",
    "Meta de Margem",
    "% Margem Bruta",
    "% Comissão Margem",
]


def format_machine_display_df(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()
    for column in CURRENCY_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].apply(format_currency_br)
    for column in PERCENT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].apply(format_percent_br)
    return display_df


def render_machine_audit(
    df_machine: pd.DataFrame,
    df_source_audit: pd.DataFrame | None,
    df_incentive_audit: pd.DataFrame | None,
) -> None:
    if df_source_audit is None or df_incentive_audit is None:
        return

    st.markdown('<div class="section-title">Auditoria da Extração</div>', unsafe_allow_html=True)
    st.caption("Conferência independente entre os selects base no SQL Server e a tabela única gerada.")

    table_summary = (
        df_machine.groupby("Tipo", dropna=False)
        .agg(
            **{
                "Qtd Linhas Tabela Única": ("Tipo", "size"),
                "Receita Bruta Tabela Única": ("Receita Bruta", lambda series: pd.to_numeric(series, errors="coerce").sum()),
            }
        )
        .reset_index()
    )
    total_row = pd.DataFrame(
        [
            {
                "Tipo": "Total",
                "Qtd Linhas Tabela Única": table_summary["Qtd Linhas Tabela Única"].sum(),
                "Receita Bruta Tabela Única": table_summary["Receita Bruta Tabela Única"].sum(),
            }
        ]
    )
    table_summary = pd.concat([table_summary, total_row], ignore_index=True)

    source_comparison = df_source_audit.merge(table_summary, on="Tipo", how="outer").fillna(0)
    source_comparison["Diferença Linhas"] = (
        source_comparison["Qtd Linhas Tabela Única"] - source_comparison["Qtd Linhas SQL"]
    )
    source_comparison["Diferença Receita Bruta"] = (
        source_comparison["Receita Bruta Tabela Única"] - source_comparison["Receita Bruta SQL"]
    )

    total_source = source_comparison[source_comparison["Tipo"] == "Total"].iloc[0]
    col_lines, col_revenue, col_incentive = st.columns(3)
    col_lines.metric(
        "Diferença linhas",
        int(total_source["Diferença Linhas"]),
        help="Tabela única menos select simples das tabelas de origem.",
    )
    col_revenue.metric(
        "Diferença Receita Bruta",
        format_currency_br(total_source["Diferença Receita Bruta"]),
        help="Tabela única menos select simples das tabelas de origem.",
    )

    incentive_audit = df_incentive_audit.iloc[0]
    incentive_audit_value = lambda column: incentive_audit.get(column, 0)
    unique_machine_incentives = (
        df_machine.drop_duplicates(subset=["Nro Chassi"])["Valor Incentivo"].pipe(pd.to_numeric, errors="coerce").sum()
    )
    row_machine_incentives = pd.to_numeric(df_machine["Valor Incentivo"], errors="coerce").sum()
    incentive_diff = unique_machine_incentives - incentive_audit_value("Valor Incentivo SQL")
    col_incentive.metric(
        "Diferença Incentivos",
        format_currency_br(incentive_diff),
        help="Tabela única por chassis únicos menos select simples de incentivos.",
    )

    display_source = source_comparison.copy()
    for column in ["Receita Bruta SQL", "Receita Bruta Tabela Única", "Diferença Receita Bruta"]:
        display_source[column] = display_source[column].apply(format_currency_br)
    for column in ["Qtd Linhas SQL", "Qtd Linhas Tabela Única", "Diferença Linhas"]:
        display_source[column] = display_source[column].astype(int)

    incentive_comparison = pd.DataFrame(
        [
            {
                "Critério": "Select SQL por chassis únicos",
                "Qtd Chassis": int(incentive_audit_value("Qtd Chassis SQL")),
                "Qtd Chassis com Incentivo": int(incentive_audit_value("Qtd Chassis com Incentivo SQL")),
                "Qtd Títulos Incentivo": int(incentive_audit_value("Qtd Títulos Incentivo SQL")),
                "Valor Incentivo": incentive_audit_value("Valor Incentivo SQL"),
                "Valor Incentivo com Título": incentive_audit_value("Valor Incentivo com Título SQL"),
                "Valor Incentivo sem Título": incentive_audit_value("Valor Incentivo sem Título SQL"),
            },
            {
                "Critério": "Tabela única por chassis únicos",
                "Qtd Chassis": int(df_machine["Nro Chassi"].nunique()),
                "Qtd Chassis com Incentivo": int(
                    df_machine.loc[
                        pd.to_numeric(df_machine["Valor Incentivo"], errors="coerce").fillna(0) != 0,
                        "Nro Chassi",
                    ].nunique()
                ),
                "Qtd Títulos Incentivo": 0,
                "Valor Incentivo": unique_machine_incentives,
                "Valor Incentivo com Título": unique_machine_incentives,
                "Valor Incentivo sem Título": 0.0,
            },
            {
                "Critério": "Tabela única por linhas",
                "Qtd Chassis": len(df_machine),
                "Qtd Chassis com Incentivo": int(
                    (pd.to_numeric(df_machine["Valor Incentivo"], errors="coerce").fillna(0) != 0).sum()
                ),
                "Qtd Títulos Incentivo": 0,
                "Valor Incentivo": row_machine_incentives,
                "Valor Incentivo com Título": row_machine_incentives,
                "Valor Incentivo sem Título": 0.0,
            },
        ]
    )
    for column in ["Valor Incentivo", "Valor Incentivo com Título", "Valor Incentivo sem Título"]:
        incentive_comparison[column] = incentive_comparison[column].apply(format_currency_br)

    with st.expander("Ver detalhes da auditoria"):
        st.markdown("**Linhas e Receita Bruta**")
        st.dataframe(display_source, use_container_width=True, hide_index=True)
        st.markdown("**Incentivos**")
        st.dataframe(incentive_comparison, use_container_width=True, hide_index=True)


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
            options=["Apurações", "Auditoria", "Comissões pagas", "Relatórios", "Configurações"],
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
        st.metric("Total pago", format_currency_br(total_paid))
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


def infer_audit_period_label(file_name: str, period_labels: list[str], default_label: str) -> str:
    match = re.search(r"(\d{2})[-_/](\d{2})[-_/](\d{4})", file_name)
    if not match:
        return default_label

    month = int(match.group(2))
    year = int(match.group(3))
    inferred = f"{MONTH_NAMES_PT.get(month, '')}/{year}"
    return inferred if inferred in period_labels else default_label


def render_paid_audit_view() -> None:
    st.markdown('<div class="section-title">Auditoria de Comissões Pagas</div>', unsafe_allow_html=True)
    st.caption(
        "Validação dos relatórios da aba 'Analitico CEN' antes de gravar na base de comissões pagas."
    )

    today = date.today()
    periods = build_period_options(today, years_back=3, years_ahead=1)
    period_labels = [item.label for item in periods]
    default_label = default_base_period(today).label

    with st.expander("Schema Postgres da tabela comissoespagas", expanded=False):
        if st.button("Validar colunas no Postgres", use_container_width=True):
            try:
                schema_status = get_paid_commissions_schema_status()
                st.session_state.paid_audit_schema_status = schema_status
            except Exception as exc:
                st.error(f"Não foi possível validar o schema no Postgres: {exc}")
        schema_status = st.session_state.get("paid_audit_schema_status")
        if schema_status is not None:
            missing_count = int((schema_status["status"] != "OK").sum())
            if missing_count:
                st.error(f"{missing_count} colunas esperadas não foram encontradas.")
            else:
                st.success("Colunas esperadas encontradas em comissoespagas.")
            st.dataframe(schema_status, use_container_width=True, hide_index=True, height=260)

    st.divider()
    st.markdown("### Regras para validação")
    col_fat_rules, col_margin_rules = st.columns(2)
    with col_fat_rules:
        fat_rules_file = st.file_uploader(
            "Regras de comissão sobre faturamento",
            type=["xlsx", "xls"],
            key="paid_audit_fat_rules",
        )
        fat_rules_df = None
        if fat_rules_file is not None:
            fat_rules_df = pd.read_excel(fat_rules_file)
            st.caption(f"{len(fat_rules_df)} linhas carregadas. Duplicidades serão preservadas.")
            if st.button("Subir regras de faturamento para Postgres", use_container_width=True):
                try:
                    count = append_model_fat_rates(fat_rules_df)
                    st.success(f"{count} linhas de regras de faturamento inseridas.")
                except Exception as exc:
                    st.error(f"Não foi possível subir regras de faturamento: {exc}")

    with col_margin_rules:
        margin_rules_file = st.file_uploader(
            "Regras de comissão sobre margem",
            type=["xlsx", "xls"],
            key="paid_audit_margin_rules",
        )
        margin_rules_df = None
        if margin_rules_file is not None:
            margin_rules_df = pd.read_excel(margin_rules_file)
            st.caption(f"{len(margin_rules_df)} linhas carregadas. Duplicidades serão preservadas.")
            if st.button("Subir regras de margem para Postgres", use_container_width=True):
                try:
                    count = append_model_margin_rates(margin_rules_df)
                    st.success(f"{count} linhas de regras de margem inseridas.")
                except Exception as exc:
                    st.error(f"Não foi possível subir regras de margem: {exc}")

    fat_lookup = build_rate_lookup(fat_rules_df, FAT_RATE_PERCENT_COLUMNS) if fat_rules_df is not None else {}
    margin_lookup = build_rate_lookup(margin_rules_df, MARGIN_RATE_PERCENT_COLUMNS) if margin_rules_df is not None else {}

    st.divider()
    st.markdown("### Arquivos de comissionamento")
    uploaded_files = st.file_uploader(
        "Relatórios de comissões pagas",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        key="paid_audit_commission_files",
        help="A aba lida será sempre 'Analitico CEN'.",
    )

    if not uploaded_files:
        st.info("Suba um ou mais relatórios para iniciar a auditoria.")
        return

    uploaded_names = [uploaded_file.name for uploaded_file in uploaded_files]
    if st.session_state.get("paid_audit_uploaded_names") != uploaded_names:
        st.session_state.paid_audit_uploaded_names = uploaded_names
        st.session_state.paid_audit_results = None

    period_rows = []
    previous_periods = st.session_state.get("paid_audit_period_by_file", {})
    for uploaded_file in uploaded_files:
        period_rows.append(
            {
                "Arquivo": uploaded_file.name,
                "Competência": previous_periods.get(
                    uploaded_file.name,
                    infer_audit_period_label(uploaded_file.name, period_labels, default_label),
                ),
            }
        )

    st.markdown("**Competência por arquivo**")
    period_df = pd.DataFrame(period_rows)
    edited_period_df = st.data_editor(
        period_df,
        use_container_width=True,
        hide_index=True,
        disabled=["Arquivo"],
        column_config={
            "Competência": st.column_config.SelectboxColumn(
                "Competência",
                options=period_labels,
                required=True,
            )
        },
        key="paid_audit_period_editor",
    )
    updated_periods = dict(st.session_state.get("paid_audit_period_by_file", {}))
    updated_periods.update(dict(zip(edited_period_df["Arquivo"], edited_period_df["Competência"])))
    st.session_state.paid_audit_period_by_file = updated_periods

    if st.button("Iniciar auditoria", type="primary", use_container_width=True):
        period_by_file = dict(zip(edited_period_df["Arquivo"], edited_period_df["Competência"]))
        st.session_state.paid_audit_period_by_file.update(period_by_file)

        extraction_keys = build_extraction_key_set(st.session_state.get("machine_df"))
        if not extraction_keys:
            st.warning("Nenhuma extração atual está carregada. Extraia o mês em Apurações antes de aprovar arquivos.")

        audit_results = []
        for uploaded_file in uploaded_files:
            try:
                uploaded_file.seek(0)
                df_report = load_commission_spreadsheet(uploaded_file)
                result = validate_paid_commission_file(
                    uploaded_file.name,
                    df_report,
                    fat_lookup,
                    margin_lookup,
                    extraction_keys,
                )
                audit_results.append(result)
            except Exception as exc:
                audit_results.append(
                    validate_paid_commission_file(
                        uploaded_file.name,
                        pd.DataFrame(),
                        fat_lookup,
                        margin_lookup,
                        extraction_keys,
                    )
                )
                audit_results[-1].issues.loc[len(audit_results[-1].issues)] = {
                    "Arquivo": uploaded_file.name,
                    "Linha": "",
                    "Severidade": "Erro",
                    "Regra": "Leitura do arquivo",
                    "Detalhe": str(exc),
                }
                audit_results[-1].passed = False
                audit_results[-1].error_count += 1

        st.session_state.paid_audit_results = audit_results

    audit_results = st.session_state.get("paid_audit_results")
    if not audit_results:
        st.info("Revise a competência de cada arquivo e clique em Iniciar auditoria.")
        return

    summary_rows = []
    period_by_file = st.session_state.get("paid_audit_period_by_file", {})
    for result in audit_results:
        summary_rows.append(
            {
                "Selecionar": result.passed,
                "Arquivo": result.file_name,
                "Competência": period_by_file.get(result.file_name, default_label),
                "Status": "Aprovado" if result.passed else "Reprovado",
                "Linhas": result.row_count,
                "Erros": result.error_count,
                "Avisos": result.warning_count,
                "Valor Comissão Total": result.total_commission,
                "6125J usado": ", ".join(f"{value:.2f}%" for value in result.summary.get("used_6125j_percentages", [])),
                "6125J regra": ", ".join(f"{value:.2f}%" for value in result.summary.get("expected_6125j_percentages", [])),
            }
        )

    select_all = st.checkbox("Selecionar todos os arquivos para upload", value=False)
    summary_df = pd.DataFrame(summary_rows)
    if select_all:
        summary_df["Selecionar"] = True

    display_summary = summary_df.copy()
    display_summary["Valor Comissão Total"] = display_summary["Valor Comissão Total"].apply(format_currency_br)
    edited_summary = st.data_editor(
        display_summary,
        use_container_width=True,
        hide_index=True,
        disabled=[column for column in display_summary.columns if column != "Selecionar"],
        column_config={"Selecionar": st.column_config.CheckboxColumn("Selecionar")},
        key="paid_audit_file_selector",
    )

    result_by_name = {result.file_name: result for result in audit_results}
    selected_names = edited_summary.loc[edited_summary["Selecionar"] == True, "Arquivo"].tolist()
    selected_results = [result_by_name[name] for name in selected_names]
    failed_selected = [result for result in selected_results if not result.passed]

    all_issues = pd.concat(
        [result.issues for result in audit_results if not result.issues.empty],
        ignore_index=True,
    ) if any(not result.issues.empty for result in audit_results) else pd.DataFrame()

    with st.expander("Detalhes das validações", expanded=not all_issues.empty):
        if all_issues.empty:
            st.success("Nenhuma inconsistência encontrada.")
        else:
            st.dataframe(all_issues, use_container_width=True, hide_index=True, height=320)

    allow_failed_upload = False
    if failed_selected:
        st.warning(
            f"{len(failed_selected)} arquivo(s) selecionado(s) não passaram na auditoria. "
            "Marque a confirmação abaixo somente se quiser gravar mesmo assim."
        )
        allow_failed_upload = st.checkbox(
            "Confirmo que quero subir arquivo(s) reprovado(s) para comissoespagas",
            value=False,
        )

    if st.button("Subir arquivos selecionados para Postgres", type="primary", use_container_width=True):
        if not selected_results:
            st.warning("Selecione ao menos um arquivo para upload.")
            return
        if failed_selected and not allow_failed_upload:
            st.error("Upload bloqueado. Confirme explicitamente para subir arquivos reprovados.")
            return

        try:
            schema_status = get_paid_commissions_schema_status()
            missing_schema = schema_status[schema_status["status"] != "OK"]
            if not missing_schema.empty:
                st.error("Upload bloqueado. A tabela comissoespagas não está com as colunas esperadas.")
                st.dataframe(missing_schema, use_container_width=True, hide_index=True)
                return

            total_saved = 0
            for result in selected_results:
                result_period_label = period_by_file.get(result.file_name, default_label)
                result_period = periods[period_labels.index(result_period_label)]
                total_saved += save_paid_commissions(
                    normalize_commission_report_df(result.dataframe),
                    competence_year=result_period.base_year,
                    competence_month=result_period.base_month,
                    period_label=result_period.label,
                    period_start=result_period.start_date,
                    period_end=result_period.end_date,
                    source="auditoria_excel",
                    file_name=result.file_name,
                )
            st.success(f"{total_saved} linhas gravadas em comissoespagas a partir dos arquivos selecionados.")
        except Exception as exc:
            st.error(f"Não foi possível subir os arquivos selecionados: {exc}")


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
        format_currency_br(pd.to_numeric(df_planilha["Valor Comissão Total"], errors="coerce").sum()),
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
    k5.metric("Valor Apto", format_currency_br(val_apta))
    k6.metric("Valor Bloqueado", format_currency_br(val_n_apta))

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
                df_source_audit = extract_machine_source_audit(
                    conn,
                    selected_period.start_date,
                    selected_period.end_date,
                )
                df_incentive_audit = extract_machine_incentive_audit(
                    conn,
                    selected_period.start_date,
                    selected_period.end_date,
                )
            conn.close()
            st.session_state.machine_df = df_machine
            st.session_state.incentive_titles_df = df_incentives
            st.session_state.machine_source_audit_df = df_source_audit
            st.session_state.machine_incentive_audit_df = df_incentive_audit
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
    k2.metric("Receita Bruta", format_currency_br(total_receita))
    k3.metric("Margem R$", format_currency_br(total_margem))
    k4.metric("Incentivos", format_currency_br(total_incentivo))
    k5.metric("Comissão Total", format_currency_br(total_comissao))

    render_machine_audit(
        df_machine,
        st.session_state.get("machine_source_audit_df"),
        st.session_state.get("machine_incentive_audit_df"),
    )

    editable_df = format_machine_display_df(df_machine)
    if "Pagar" not in editable_df.columns:
        editable_df.insert(0, "Pagar", False)

    edited_df = st.data_editor(
        editable_df,
        use_container_width=True,
        height=450,
        disabled=[column for column in editable_df.columns if column != "Pagar"],
        column_config={
            "Pagar": st.column_config.CheckboxColumn("Pagar", default=False),
        },
        key="machine_payment_editor",
    )
    selected_indexes = edited_df.index[edited_df["Pagar"] == True]
    selected_to_pay = df_machine.loc[selected_indexes]
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
            format_machine_display_df(df_incentives),
            use_container_width=True,
            height=350,
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
    if "machine_source_audit_df" not in st.session_state:
        st.session_state.machine_source_audit_df = None
    if "machine_incentive_audit_df" not in st.session_state:
        st.session_state.machine_incentive_audit_df = None
    if "paid_commissions_lookup" not in st.session_state:
        st.session_state.paid_commissions_lookup = None
    if "paid_audit_schema_status" not in st.session_state:
        st.session_state.paid_audit_schema_status = None
    if "paid_audit_uploaded_names" not in st.session_state:
        st.session_state.paid_audit_uploaded_names = []
    if "paid_audit_results" not in st.session_state:
        st.session_state.paid_audit_results = None
    if "paid_audit_period_by_file" not in st.session_state:
        st.session_state.paid_audit_period_by_file = {}

    render_header()
    selected_view = render_sidebar()

    if selected_view == "Apurações":
        render_machine_extraction()
    elif selected_view == "Auditoria":
        render_paid_audit_view()
    elif selected_view == "Comissões pagas":
        render_paid_commissions_view()
    elif selected_view == "Relatórios":
        render_reports_view()
    elif selected_view == "Configurações":
        render_settings_view()
