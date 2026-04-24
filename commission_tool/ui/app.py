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
from commission_tool.core.apuracao import apply_frontend_default_fat_commission, prepare_machine_apuracao
from commission_tool.core.eligibility import diagnose_key_formats, run_eligibility_validation
from commission_tool.core.formatting import format_currency_br, format_percent_br
from commission_tool.core.paid_audit import (
    FAT_RATE_PERCENT_COLUMNS,
    MARGIN_RATE_PERCENT_COLUMNS,
    build_extraction_key_set,
    build_margin_rule_lookup,
    build_rate_lookup,
    normalize_commission_report_df,
    validate_paid_commission_file,
)
from commission_tool.core.periods import MONTH_NAMES_PT, build_period_options, default_base_period
from commission_tool.core.reports import (
    build_cen_report,
    build_manager_report,
    build_used_implements_coordinator_report,
)
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
    read_manager_relations,
    read_excluded_commission_chassis_summary,
    read_model_fat_rates,
    read_model_margin_rates,
    read_paid_commission_period_labels,
    read_paid_commissions_by_period_label,
    read_paid_commission_chassis_summary,
    read_paid_commissions,
    replace_active_manager_relations,
    replace_active_model_fat_rates,
    replace_active_model_margin_rates,
    save_excluded_commissions,
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
    "Valor Pago Histórico",
    "Valor Estornado Histórico",
    "Saldo Comissão Paga Chassi",
    "Comissão Faturamento",
    "Venda Direta",
    "Comissão Usados",
    "Comissão Implementos",
    "Comissão Invasão de área",
    "Comissão Margem",
    "Comissão Total CEN",
    "Comissão Gerente",
    "Valor Total da Comissão",
]

PERCENT_COLUMNS = [
    "% Comissão Fat.",
    "% Margem Direta",
    "Meta de Margem",
    "% Margem Bruta",
    "% Comissão Margem",
    "Meta Margem",
    "% MB Realizado",
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


def build_machine_selection_editor_df(
    df_machine: pd.DataFrame,
    selection_state: dict[int, dict[str, bool]] | None = None,
) -> pd.DataFrame:
    selection_state = selection_state or {}
    editable_df = format_machine_display_df(df_machine).copy()
    pagar_values = []
    excluir_values = []
    for idx in editable_df.index:
        row_state = selection_state.get(int(idx), {})
        pagar_values.append(bool(row_state.get("pagar", False)))
        excluir_values.append(bool(row_state.get("excluir", False)))

    if "Pagar" in editable_df.columns:
        editable_df["Pagar"] = pagar_values
    else:
        editable_df.insert(0, "Pagar", pagar_values)

    if "Excluir" in editable_df.columns:
        editable_df["Excluir"] = excluir_values
    else:
        editable_df.insert(1, "Excluir", excluir_values)

    return editable_df


def merge_machine_selection_state(
    current_state: dict[int, dict[str, bool]] | None,
    edited_df: pd.DataFrame | None,
) -> dict[int, dict[str, bool]]:
    merged_state = dict(current_state or {})
    if edited_df is None or edited_df.empty:
        return merged_state

    for idx, row in edited_df.iterrows():
        row_state = {
            "pagar": bool(row.get("Pagar", False)),
            "excluir": bool(row.get("Excluir", False)),
        }
        if row_state["pagar"] or row_state["excluir"]:
            merged_state[int(idx)] = row_state
        else:
            merged_state.pop(int(idx), None)
    return merged_state


def get_machine_selected_rows(
    df_machine: pd.DataFrame,
    selection_state: dict[int, dict[str, bool]] | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Index, pd.Index]:
    selection_state = selection_state or {}
    valid_indexes = set(int(idx) for idx in df_machine.index)
    pay_indexes = sorted(
        idx for idx, state in selection_state.items()
        if idx in valid_indexes and state.get("pagar", False)
    )
    exclude_indexes = sorted(
        idx for idx, state in selection_state.items()
        if idx in valid_indexes and state.get("excluir", False)
    )
    pay_index = pd.Index(pay_indexes, dtype="int64")
    exclude_index = pd.Index(exclude_indexes, dtype="int64")
    selected_to_pay = df_machine.loc[pay_index] if len(pay_index) else df_machine.iloc[0:0].copy()
    selected_to_exclude = df_machine.loc[exclude_index] if len(exclude_index) else df_machine.iloc[0:0].copy()
    return selected_to_pay, selected_to_exclude, pay_index, exclude_index


def build_machine_pay_review_df(selected_to_pay: pd.DataFrame) -> pd.DataFrame:
    pay_review = selected_to_pay.copy().reset_index(drop=True)
    if pay_review.empty:
        return pay_review
    pay_review.insert(0, "__pay_review_row_id", pay_review.index.astype(int))
    pay_review.insert(1, "Confirmar Pagamento", True)
    return pay_review


def get_confirmed_pay_rows(
    pay_review_df: pd.DataFrame | None,
    edited_pay_review: pd.DataFrame | None,
) -> pd.DataFrame:
    if pay_review_df is None or pay_review_df.empty or edited_pay_review is None or edited_pay_review.empty:
        return pd.DataFrame()

    confirmed_ids = pd.to_numeric(
        edited_pay_review.loc[
            edited_pay_review["Confirmar Pagamento"] == True,
            "__pay_review_row_id",
        ],
        errors="coerce",
    ).dropna().astype(int)

    if confirmed_ids.empty:
        return pay_review_df.iloc[0:0].drop(columns=["Confirmar Pagamento", "__pay_review_row_id"], errors="ignore")

    return pay_review_df.loc[
        pay_review_df["__pay_review_row_id"].isin(confirmed_ids)
    ].drop(columns=["Confirmar Pagamento", "__pay_review_row_id"], errors="ignore")


def sanitize_download_label(label: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", str(label or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned or "periodo"


def refresh_machine_apuracao_state() -> None:
    df_machine_raw = st.session_state.get("machine_raw_df")
    if df_machine_raw is None:
        return

    df_fat_rates = st.session_state.get("machine_rate_fat_df")
    if df_fat_rates is None:
        df_fat_rates = read_model_fat_rates()

    df_margin_rates = st.session_state.get("machine_rate_margin_df")
    if df_margin_rates is None:
        df_margin_rates = read_model_margin_rates()

    df_manager_relations = st.session_state.get("manager_relations_df")
    if df_manager_relations is None:
        df_manager_relations = read_manager_relations()

    df_paid_summary = read_paid_commission_chassis_summary()
    df_excluded_summary = read_excluded_commission_chassis_summary()
    df_machine, df_machine_history = prepare_machine_apuracao(
        df_machine_raw,
        df_fat_rates,
        df_margin_rates,
        df_paid_summary,
        df_manager_relations,
        df_excluded_summary,
    )
    st.session_state.machine_df = df_machine
    st.session_state.machine_full_history_df = df_machine_history
    st.session_state.machine_paid_summary_df = df_paid_summary
    st.session_state.machine_excluded_summary_df = df_excluded_summary
    st.session_state.manager_relations_df = df_manager_relations
    st.session_state.machine_receivable_validation_df = None
    st.session_state.machine_pay_review_df = None
    st.session_state.machine_exclude_review_df = None
    st.session_state.machine_selection_state = {}


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
            <h1>✅ Apuração de Comissões</h1>
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
            "data_emissao",
            "nro_chassi",
            "nome_cliente",
            "cen",
            "valor_comissao_total",
        ]
        sidebar_cols = [col for col in sidebar_cols if col in df_paid.columns]
        df_paid_display = df_paid[sidebar_cols].copy()
        if "valor_comissao_total" in df_paid_display.columns:
            df_paid_display["valor_comissao_total"] = df_paid_display["valor_comissao_total"].apply(format_currency_br)
        st.dataframe(
            df_paid_display,
            use_container_width=True,
            height=420,
        )

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
    margin_lookup = build_margin_rule_lookup(margin_rules_df) if margin_rules_df is not None else {}

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
                "Linhas lidas": result.summary.get("loaded_row_count", result.row_count),
                "Colunas lidas": result.summary.get("loaded_column_count", len(result.dataframe.columns)),
                "Linhas validadas": result.row_count,
                "Receita Bruta lida": result.summary.get("loaded_revenue", 0.0),
                "Erros": result.error_count,
                "Avisos": result.warning_count,
                "Valor Comissão Total lido": result.summary.get("loaded_total_commission", result.total_commission),
                "6125J usado": ", ".join(f"{value:.2f}%" for value in result.summary.get("used_6125j_percentages", [])),
                "6125J regra": ", ".join(f"{value:.2f}%" for value in result.summary.get("expected_6125j_percentages", [])),
            }
        )

    select_all = st.checkbox("Selecionar todos os arquivos para upload", value=False)
    summary_df = pd.DataFrame(summary_rows)
    if select_all:
        summary_df["Selecionar"] = True

    display_summary = summary_df.copy()
    for column in ["Receita Bruta lida", "Valor Comissão Total lido"]:
        display_summary[column] = display_summary[column].apply(format_currency_br)
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
    st.caption("Extração histórica de faturamentos e devoluções, com cálculo e confronto de comissões pagas.")

    today = date.today()
    extraction_start = date(2024, 11, 1)
    periods = build_period_options(today, years_back=1, years_ahead=1)
    labels = [period.label for period in periods]
    default_label = default_base_period(today).label
    default_index = labels.index(default_label) if default_label in labels else 0

    col_period, col_action = st.columns([2, 1])
    with col_period:
        selected_label = st.selectbox(
            "Competência da comissão",
            options=labels,
            index=default_index,
        )
    selected_period = periods[labels.index(selected_label)]

    st.info(
        f"Competência: **{selected_period.label}** | "
        "Janela 16-15: "
        f"**{selected_period.start_date.strftime('%d/%m/%Y')}** até "
        f"**{selected_period.end_date.strftime('%d/%m/%Y')}** | "
        "Extração histórica: "
        f"**{extraction_start.strftime('%d/%m/%Y')}** até "
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
                df_machine_raw = extract_machine_commission_base(
                    conn,
                    extraction_start,
                    selected_period.end_date,
                )
                df_incentives = extract_incentive_titles(conn)
                df_source_audit = extract_machine_source_audit(
                    conn,
                    extraction_start,
                    selected_period.end_date,
                )
                df_incentive_audit = extract_machine_incentive_audit(
                    conn,
                    extraction_start,
                    selected_period.end_date,
                )
            conn.close()
            with st.spinner("Aplicando regras do Postgres e confrontando histórico pago..."):
                df_fat_rates = read_model_fat_rates()
                df_margin_rates = read_model_margin_rates()
                df_manager_relations = read_manager_relations()
                df_paid_summary = read_paid_commission_chassis_summary()
                df_excluded_summary = read_excluded_commission_chassis_summary()
                df_machine, df_machine_history = prepare_machine_apuracao(
                    df_machine_raw,
                    df_fat_rates,
                    df_margin_rates,
                    df_paid_summary,
                    df_manager_relations,
                    df_excluded_summary,
                )
            st.session_state.machine_df = df_machine
            st.session_state.machine_raw_df = df_machine_raw
            st.session_state.machine_full_history_df = df_machine_history
            st.session_state.machine_rate_fat_df = df_fat_rates
            st.session_state.machine_rate_margin_df = df_margin_rates
            st.session_state.manager_relations_df = df_manager_relations
            st.session_state.machine_paid_summary_df = df_paid_summary
            st.session_state.machine_excluded_summary_df = df_excluded_summary
            st.session_state.incentive_titles_df = df_incentives
            st.session_state.machine_source_audit_df = df_source_audit
            st.session_state.machine_incentive_audit_df = df_incentive_audit
            st.session_state.machine_period_label = selected_period.label
            st.session_state.machine_period_start = selected_period.start_date
            st.session_state.machine_period_end = selected_period.end_date
            st.session_state.machine_extraction_start = extraction_start
            st.session_state.machine_receivable_validation_df = None
            st.session_state.machine_pay_review_df = None
            st.session_state.machine_exclude_review_df = None
            st.session_state.machine_selection_state = {}
            bloqueados_pagamento = int(
                pd.to_numeric(
                    df_machine_history.get("Bloqueado por Pagamento Histórico"),
                    errors="coerce",
                ).fillna(0).sum()
            )
            bloqueados_exclusao = int(
                pd.to_numeric(
                    df_machine_history.get("Bloqueado por Exclusão"),
                    errors="coerce",
                ).fillna(0).sum()
            )
            st.success(
                f"Extração concluída: {len(df_machine)} registros para apuração "
                f"({bloqueados_pagamento} bloqueados por pagamento histórico positivo | "
                f"{bloqueados_exclusao} excluídos manualmente)."
            )
        except Exception as exc:
            st.error(f"Erro durante extração de máquinas: {exc}")

    df_machine = st.session_state.get("machine_df")
    if df_machine is None:
        return
    df_machine_full = st.session_state.get("machine_full_history_df")
    if df_machine_full is None:
        df_machine_full = df_machine

    use_default_fat_rate = st.toggle(
        "Aplicar padrão de 1% quando % Comissão Fat. = 0",
        value=st.session_state.get("machine_default_one_percent_enabled", True),
        key="machine_default_one_percent_enabled",
        help="Regra visual da seção Apurações. Não altera as tabelas de configuração no Postgres.",
    )
    previous_default_toggle = st.session_state.get("machine_default_one_percent_last")
    if previous_default_toggle is None:
        st.session_state.machine_default_one_percent_last = use_default_fat_rate
    elif previous_default_toggle != use_default_fat_rate:
        st.session_state.machine_default_one_percent_last = use_default_fat_rate
        st.session_state.machine_receivable_validation_df = None
        st.session_state.machine_pay_review_df = None
        st.session_state.machine_exclude_review_df = None
        st.session_state.machine_selection_state = {}
        st.info("A validação e as revisões foram limpas porque a regra padrão de 1% foi alterada.")

    df_machine = apply_frontend_default_fat_commission(df_machine, use_default_fat_rate)
    default_applied_count = int(
        pd.to_numeric(
            df_machine.get("Padrão 1% Fat. Aplicado", pd.Series([False] * len(df_machine), index=df_machine.index)),
            errors="coerce",
        ).fillna(0).sum()
    )

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

    bloqueados = int(
        pd.to_numeric(
            df_machine_full.get(
                "Bloqueado por Pagamento Histórico",
                pd.Series([False] * len(df_machine_full), index=df_machine_full.index),
            ),
            errors="coerce",
        ).fillna(0).sum()
    )
    sem_regra_fat = (
        int((df_machine.get("Regra Comissão Fat. Encontrada") == False).sum())
        if "Regra Comissão Fat. Encontrada" in df_machine.columns
        else 0
    )
    sem_regra_margem = (
        int((df_machine.get("Regra Comissão Margem Encontrada") == False).sum())
        if "Regra Comissão Margem Encontrada" in df_machine.columns
        else 0
    )
    st.caption(
        f"Base histórica extraída: {len(df_machine_full)} linhas | "
        f"Bloqueadas por pagamento histórico positivo: {bloqueados} | "
        f"Excluídas manualmente: "
        f"{int(pd.to_numeric(df_machine_full.get('Bloqueado por Exclusão'), errors='coerce').fillna(0).sum())} | "
        f"Sem regra fat.: {sem_regra_fat} | Sem regra margem: {sem_regra_margem} | "
        f"Padrão 1% aplicado: {default_applied_count}"
    )

    export_period_label = sanitize_download_label(st.session_state.get("machine_period_label", selected_period.label))
    export_buffer = dataframe_to_excel_download(df_machine, sheet_name="Apuração Máquinas")
    st.download_button(
        "Exportar apuração em .xlsx",
        data=export_buffer,
        file_name=f"apuracao_maquinas_{export_period_label}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
    )

    render_machine_audit(
        df_machine_full,
        st.session_state.get("machine_source_audit_df"),
        st.session_state.get("machine_incentive_audit_df"),
    )

    with st.expander("Histórico confrontado com comissões pagas"):
        history_columns = [
            "Tipo",
            "Filial",
            "Data de Emissão",
            "Nro Documento",
            "Modelo",
            "Nro Chassi",
            "Valor Comissão Total",
            "Qtd Lançamentos Pagos",
            "Valor Pago Histórico",
            "Valor Estornado Histórico",
            "Saldo Comissão Paga Chassi",
            "Qtd Lançamentos Excluídos",
            "Bloqueado por Exclusão",
            "Status Confronto Pagas",
            "Bloqueado por Pagamento Histórico",
        ]
        history_columns = [column for column in history_columns if column in df_machine_full.columns]
        st.dataframe(
            format_machine_display_df(df_machine_full[history_columns]),
            use_container_width=True,
            height=320,
        )

    cen_filter_options = []
    if "CEN" in df_machine.columns:
        cen_filter_options = sorted(
            value for value in df_machine["CEN"].fillna("").astype(str).str.strip().unique().tolist() if value
        )

    selected_cens = st.multiselect(
        "Filtrar por CEN",
        options=cen_filter_options,
        default=[],
        help="Filtro apenas visual. As marcações de pagar e excluir continuam valendo mesmo ao trocar o filtro.",
    )

    selection_state = st.session_state.get("machine_selection_state", {})
    editable_df = build_machine_selection_editor_df(df_machine, selection_state)
    visible_df = editable_df
    if selected_cens:
        visible_df = editable_df[
            editable_df["CEN"].fillna("").astype(str).str.strip().isin(selected_cens)
        ]

    edited_df = st.data_editor(
        visible_df,
        use_container_width=True,
        height=450,
        hide_index=True,
        disabled=[column for column in visible_df.columns if column not in ["Pagar", "Excluir"]],
        column_config={
            "Pagar": st.column_config.CheckboxColumn("Pagar", default=False),
            "Excluir": st.column_config.CheckboxColumn("Excluir", default=False),
        },
        key="machine_payment_editor",
    )
    st.session_state.machine_selection_state = merge_machine_selection_state(selection_state, edited_df)
    selected_to_pay, selected_to_exclude, selected_indexes, excluded_indexes = get_machine_selected_rows(
        df_machine,
        st.session_state.machine_selection_state,
    )
    hidden_selected_pay = max(len(selected_to_pay) - int((edited_df["Pagar"] == True).sum()) if not edited_df.empty else len(selected_to_pay), 0)
    hidden_selected_exclude = max(len(selected_to_exclude) - int((edited_df["Excluir"] == True).sum()) if not edited_df.empty else len(selected_to_exclude), 0)
    st.caption(
        f"{len(selected_to_pay)} comissões selecionadas para pagamento | "
        f"{len(selected_to_exclude)} selecionadas para exclusão."
    )
    if selected_cens:
        st.caption(
            f"Filtro ativo em {len(selected_cens)} CEN(s). "
            f"Seleções fora da visualização atual mantidas: {hidden_selected_pay} para pagar | "
            f"{hidden_selected_exclude} para excluir."
        )

    st.markdown('<div class="section-title">Validação do Contas a Receber</div>', unsafe_allow_html=True)
    col_exclude, col_validate = st.columns(2)
    with col_exclude:
        if st.button("Excluir comissões", use_container_width=True):
            conflict_indexes = selected_indexes.intersection(excluded_indexes)
            if len(conflict_indexes) > 0:
                st.warning(
                    "Existem linhas marcadas para pagar e excluir ao mesmo tempo. "
                    "Remova uma das marcações para continuar."
                )
            elif selected_to_exclude.empty:
                st.warning("Marque ao menos uma comissão para exclusão.")
            else:
                try:
                    excluded_count = save_excluded_commissions(
                        selected_to_exclude,
                        competence_year=selected_period.base_year,
                        competence_month=selected_period.base_month,
                        period_label=selected_period.label,
                        period_start=selected_period.start_date,
                        period_end=selected_period.end_date,
                        source="streamlit_exclusao",
                    )
                    refresh_machine_apuracao_state()
                    st.success(
                        f"{excluded_count} comissões gravadas em comissoesexcluidas "
                        "e removidas da apuração atual."
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Não foi possível excluir as comissões selecionadas: {exc}")
    with col_validate:
        if st.button("Validar Contas a Receber", type="primary", use_container_width=True):
            conflict_indexes = selected_indexes.intersection(excluded_indexes)
            if len(conflict_indexes) > 0:
                st.warning(
                    "Existem linhas marcadas para pagar e excluir ao mesmo tempo. "
                    "Remova uma das marcações para continuar."
                )
            elif len(excluded_indexes) > 0:
                st.warning(
                    "Existem comissões marcadas para exclusão. Clique em Excluir comissões "
                    "antes de validar o Contas a Receber."
                )
            else:
                cfg = st.session_state.conn_cfg
                validation_input = df_machine.reset_index(drop=True).copy()
                validation_input.insert(0, "__apuracao_row_id", validation_input.index)
                progress = st.progress(0)
                status = st.empty()

                def progress_callback(current: int, total: int, row: pd.Series) -> None:
                    progress.progress(current / total if total else 1.0)
                    status.text(
                        f"Validando {current}/{total} - "
                        f"{row.get('Nro Documento', '')} / {row.get('Nro Chassi', '')}"
                    )

                try:
                    conn = get_connection(**cfg)
                    validation_df = run_eligibility_validation(conn, validation_input, progress_callback)
                    conn.close()
                    st.session_state.machine_receivable_validation_df = validation_df
                    progress.empty()
                    status.empty()
                    st.success(f"Validação concluída: {len(validation_df)} registros avaliados.")
                except Exception as exc:
                    progress.empty()
                    status.empty()
                    st.error(f"Erro durante validação do Contas a Receber: {exc}")

    if st.button("Garantir tabelas Postgres", use_container_width=True):
        try:
            ensure_commission_tables()
            st.success("Tabelas Postgres prontas.")
        except Exception as exc:
            st.error(f"Não foi possível preparar as tabelas: {exc}")

    validation_df = st.session_state.get("machine_receivable_validation_df")
    if validation_df is not None and not validation_df.empty:
        total_validado = len(validation_df)
        aptos = int((validation_df["Status Geral"] == STATUS_APTO).sum())
        nao_aptos = int((validation_df["Status Geral"] == STATUS_NAO_APTO).sum())
        verificar = int((validation_df["Status Geral"] == STATUS_VERIFICAR).sum())
        v1_pendentes = int(validation_df["V1 - Status"].isin(["NÃO APTO", "NÃO ENCONTRADO", "ERRO"]).sum())
        v2_pendentes = int(validation_df["V2 - Status"].isin(["NÃO APTO", "NÃO ENCONTRADO", "ERRO"]).sum())

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Validados", total_validado)
        c2.metric("Aptos", aptos)
        c3.metric("Não aptos", nao_aptos)
        c4.metric("Verificar", verificar)
        c5.metric("Pendências V1/V2", f"{v1_pendentes}/{v2_pendentes}")

        status_options = [
            status
            for status in [STATUS_APTO, STATUS_NAO_APTO, STATUS_VERIFICAR]
            if status in validation_df["Status Geral"].dropna().astype(str).unique().tolist()
        ]
        selected_validation_status = st.multiselect(
            "Filtrar validação por Status Geral",
            options=status_options,
            default=[],
        )

        validation_display = validation_df.copy()
        if "__apuracao_row_id" in validation_display.columns:
            source_validation_fields = df_machine.reset_index(drop=True).copy()
            source_validation_fields.insert(0, "__apuracao_row_id", source_validation_fields.index)
            source_columns = [
                column
                for column in ["__apuracao_row_id", "Nome do Cliente", "Classificação Venda"]
                if column in source_validation_fields.columns
            ]
            source_validation_fields = source_validation_fields[source_columns]
            validation_display = validation_display.merge(
                source_validation_fields,
                on="__apuracao_row_id",
                how="left",
                suffixes=("", "_apuracao"),
            )
            if "Cliente" in validation_display.columns and "Nome do Cliente" in validation_display.columns:
                validation_display["Nome do Cliente"] = (
                    validation_display["Nome do Cliente"]
                    .fillna("")
                    .astype(str)
                    .mask(
                        validation_display["Nome do Cliente"].fillna("").astype(str).str.strip().eq(""),
                        validation_display["Cliente"],
                    )
                )
            elif "Cliente" in validation_display.columns and "Nome do Cliente" not in validation_display.columns:
                validation_display["Nome do Cliente"] = validation_display["Cliente"]

            if "Classificação Venda_apuracao" in validation_display.columns:
                if "Classificação Venda" in validation_display.columns:
                    validation_display["Classificação Venda"] = (
                        validation_display["Classificação Venda"]
                        .fillna("")
                        .astype(str)
                        .mask(
                            validation_display["Classificação Venda"].fillna("").astype(str).str.strip().eq(""),
                            validation_display["Classificação Venda_apuracao"],
                        )
                    )
                else:
                    validation_display["Classificação Venda"] = validation_display["Classificação Venda_apuracao"]
                validation_display = validation_display.drop(columns=["Classificação Venda_apuracao"])

        validation_display = validation_display.drop(columns=["__apuracao_row_id"], errors="ignore").copy()
        if "Nome do Cliente" not in validation_display.columns and "Cliente" in validation_display.columns:
            validation_display.insert(
                validation_display.columns.get_loc("Cliente") + 1,
                "Nome do Cliente",
                validation_display["Cliente"],
            )
            validation_display = validation_display.drop(columns=["Cliente"])

        if selected_validation_status:
            validation_display = validation_display[
                validation_display["Status Geral"].isin(selected_validation_status)
            ].reset_index(drop=True)

        validation_order = [
            "Status Geral",
            "Filial",
            "CEN",
            "Nome do Cliente",
            "Classificação Venda",
            "Nro Documento",
            "Nro Chassi",
            "Data de Emissão",
            "Valor Comissão",
            "V1 - Status",
            "V1 - NF Incentivo",
            "V1 - Saldo Incentivo",
            "V1 - Data Emissão",
            "V1 - Detalhe",
            "V2 - Status",
            "V2 - Cód. Cliente",
            "V2 - Saldo Cliente",
            "V2 - Tipos Título",
            "V2 - Detalhe",
        ]
        ordered_columns = [column for column in validation_order if column in validation_display.columns]
        remaining_columns = [column for column in validation_display.columns if column not in ordered_columns]
        validation_display = validation_display[ordered_columns + remaining_columns]

        export_validation = dataframe_to_excel_download(
            validation_display,
            sheet_name="Validacao Contas Receber",
        )
        export_period_label = sanitize_download_label(
            st.session_state.get("machine_period_label", selected_period.label)
        )
        st.download_button(
            "Exportar validação em .xlsx",
            data=export_validation,
            file_name=f"validacao_contas_receber_{export_period_label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )
        st.caption(f"Exibindo {len(validation_display)} de {total_validado} registros validados.")
        st.dataframe(
            format_machine_display_df(validation_display),
            use_container_width=True,
            height=320,
        )
    else:
        st.info("Rode a validação do Contas a Receber antes de gravar comissões pagas.")

    col_pay, _ = st.columns(2)
    with col_pay:
        can_prepare_review = validation_df is not None and not validation_df.empty
        if st.button(
            "Pagar comissões selecionadas",
            type="primary",
            use_container_width=True,
            disabled=not can_prepare_review,
        ):
            conflict_indexes = selected_indexes.intersection(excluded_indexes)
            if len(conflict_indexes) > 0:
                st.warning(
                    "Existem linhas marcadas para pagar e excluir ao mesmo tempo. "
                    "Remova uma das marcações para continuar."
                )
                st.session_state.machine_pay_review_df = None
                st.session_state.machine_exclude_review_df = None
            elif len(excluded_indexes) > 0:
                st.warning(
                    "Existem comissões marcadas para exclusão. Clique em Excluir comissões "
                    "antes de preparar o pagamento."
                )
                st.session_state.machine_pay_review_df = None
                st.session_state.machine_exclude_review_df = None
            elif selected_to_pay.empty:
                st.warning("Marque ao menos uma comissão para pagamento.")
            else:
                pay_review = build_machine_pay_review_df(selected_to_pay)
                st.session_state.machine_pay_review_df = pay_review
                st.session_state.machine_exclude_review_df = None
                st.success("Tabela de revisão gerada. Confira antes de gravar no banco.")

    pay_review_df = st.session_state.get("machine_pay_review_df")
    edited_pay_review = pd.DataFrame()

    if pay_review_df is not None:
        st.markdown('<div class="section-title">Revisão antes da gravação</div>', unsafe_allow_html=True)

    if pay_review_df is not None and not pay_review_df.empty:
        st.markdown("#### Comissões a pagar")
        review_receita = pd.to_numeric(pay_review_df.get("Receita Bruta"), errors="coerce").fillna(0).sum()
        review_margem = pd.to_numeric(pay_review_df.get("Margem R$"), errors="coerce").fillna(0).sum()
        review_comissao = pd.to_numeric(pay_review_df.get("Valor Comissão Total"), errors="coerce").fillna(0).sum()
        review_k1, review_k2, review_k3 = st.columns(3)
        review_k1.metric("Receita Bruta", format_currency_br(review_receita))
        review_k2.metric("Margem R$", format_currency_br(review_margem))
        review_k3.metric("Comissão Total", format_currency_br(review_comissao))
        display_pay_review = format_machine_display_df(pay_review_df)
        edited_pay_review = st.data_editor(
            display_pay_review,
            use_container_width=True,
            height=280,
            hide_index=True,
            disabled=[
                column
                for column in display_pay_review.columns
                if column not in ["Confirmar Pagamento", "__pay_review_row_id"]
            ],
            column_config={
                "Confirmar Pagamento": st.column_config.CheckboxColumn("Confirmar", default=True),
                "__pay_review_row_id": None,
            },
            key="machine_pay_review_editor",
        )

    if pay_review_df is not None:
        if st.button("Gravar comissões revisadas", type="primary", use_container_width=True):
            pay_to_save = get_confirmed_pay_rows(pay_review_df, edited_pay_review)

            try:
                paid_count = save_paid_commissions(
                    pay_to_save,
                    competence_year=selected_period.base_year,
                    competence_month=selected_period.base_month,
                    period_label=selected_period.label,
                    period_start=selected_period.start_date,
                    period_end=selected_period.end_date,
                    source="streamlit",
                )
                refresh_machine_apuracao_state()
                st.success(f"{paid_count} comissões gravadas em comissoespagas.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível gravar as comissões revisadas: {exc}")

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
                file_name=(
                    f"titulos_incentivo_"
                    f"{sanitize_download_label(st.session_state.get('machine_period_label', 'periodo'))}.xlsx"
                ),
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
    st.markdown("### Relatórios de comissões")

    try:
        available_periods = read_paid_commission_period_labels()
    except Exception as exc:
        st.error(f"Não foi possível carregar as competências do relatório: {exc}")
        return

    if not available_periods:
        st.info("Não há competências disponíveis em comissoespagas para gerar o relatório.")
        return

    selected_period = st.selectbox(
        "Competência do relatório",
        options=available_periods,
        index=0,
    )

    if st.button("Gerar relatórios", type="primary", use_container_width=True):
        try:
            paid_commissions = read_paid_commissions_by_period_label(selected_period)
            manager_relations = read_manager_relations()
            cen_report_df, pending_report_df = build_cen_report(
                paid_commissions,
                manager_relations,
                selected_period,
            )
            manager_report_df = build_manager_report(
                paid_commissions,
                manager_relations,
                selected_period,
            )
            coordinator_report_df = build_used_implements_coordinator_report(
                paid_commissions,
                selected_period,
            )
            st.session_state.cen_report_df = cen_report_df
            st.session_state.cen_pending_report_df = pending_report_df
            st.session_state.manager_report_df = manager_report_df
            st.session_state.coordinator_report_df = coordinator_report_df
            st.session_state.cen_report_period = selected_period
            st.success(
                f"Relatórios gerados para {selected_period}: "
                f"{len(cen_report_df)} vendedores, {len(manager_report_df)} gerentes, "
                f"{len(coordinator_report_df)} linhas de coordenador "
                f"e {len(pending_report_df)} pendência(s)."
            )
        except Exception as exc:
            st.error(f"Não foi possível gerar os relatórios: {exc}")
            return

    cen_report_df = st.session_state.get("cen_report_df")
    pending_report_df = st.session_state.get("cen_pending_report_df")
    manager_report_df = st.session_state.get("manager_report_df")
    coordinator_report_df = st.session_state.get("coordinator_report_df")
    report_period = st.session_state.get("cen_report_period", selected_period)

    if cen_report_df is None or manager_report_df is None or coordinator_report_df is None:
        st.info("Selecione a competência e clique em Gerar relatórios.")
        return

    st.markdown("### Relatório do CEN")
    total_cens = len(cen_report_df)
    total_report = pd.to_numeric(cen_report_df.get("Valor Comissão Total"), errors="coerce").fillna(0).sum()
    total_pending = 0.0 if pending_report_df is None else pd.to_numeric(
        pending_report_df.get("Valor Comissão Total"),
        errors="coerce",
    ).fillna(0).sum()

    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("CENs no relatório", total_cens)
    metric_2.metric("Valor Comissão Total", format_currency_br(total_report))
    metric_3.metric("Pendências", format_currency_br(total_pending))

    report_buffer = dataframe_to_excel_download(cen_report_df, sheet_name="Relatorio CEN")
    st.download_button(
        "Exportar relatório CEN em .xlsx",
        data=report_buffer,
        file_name=f"relatorio_cen_{sanitize_download_label(report_period)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
    )
    st.dataframe(
        format_machine_display_df(cen_report_df),
        use_container_width=True,
        height=420,
        hide_index=True,
    )

    st.markdown("### Relatório dos Gerentes")
    total_managers = len(manager_report_df)
    total_manager_commission = pd.to_numeric(
        manager_report_df.get("Comissão Gerente"),
        errors="coerce",
    ).fillna(0).sum()
    total_manager_revenue = pd.to_numeric(
        manager_report_df.get("Receita Bruta"),
        errors="coerce",
    ).fillna(0).sum()

    manager_metric_1, manager_metric_2, manager_metric_3 = st.columns(3)
    manager_metric_1.metric("Gerentes no relatório", total_managers)
    manager_metric_2.metric("Receita Bruta", format_currency_br(total_manager_revenue))
    manager_metric_3.metric("Comissão Gerente", format_currency_br(total_manager_commission))

    manager_buffer = dataframe_to_excel_download(manager_report_df, sheet_name="Relatorio Gerentes")
    st.download_button(
        "Exportar relatório Gerentes em .xlsx",
        data=manager_buffer,
        file_name=f"relatorio_gerentes_{sanitize_download_label(report_period)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
    )
    st.dataframe(
        format_machine_display_df(manager_report_df),
        use_container_width=True,
        height=360,
        hide_index=True,
    )

    st.markdown("### Coordenador de Seminovos e Implementos")
    total_coordinator_revenue = pd.to_numeric(
        coordinator_report_df.get("Receita Bruta"),
        errors="coerce",
    ).fillna(0).sum()
    total_coordinator_commission = pd.to_numeric(
        coordinator_report_df.get("Valor Total da Comissão"),
        errors="coerce",
    ).fillna(0).sum()

    coordinator_metric_1, coordinator_metric_2, coordinator_metric_3 = st.columns(3)
    coordinator_metric_1.metric("Tipos no relatório", len(coordinator_report_df))
    coordinator_metric_2.metric("Receita Bruta", format_currency_br(total_coordinator_revenue))
    coordinator_metric_3.metric("Valor Total da Comissão", format_currency_br(total_coordinator_commission))

    coordinator_buffer = dataframe_to_excel_download(
        coordinator_report_df,
        sheet_name="Coord Seminovos Implementos",
    )
    st.download_button(
        "Exportar relatório Coordenador em .xlsx",
        data=coordinator_buffer,
        file_name=f"relatorio_coordenador_seminovos_implementos_{sanitize_download_label(report_period)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
    )
    st.dataframe(
        format_machine_display_df(coordinator_report_df),
        use_container_width=True,
        height=260,
        hide_index=True,
    )

    st.markdown("### Pendências de relatório")
    if pending_report_df is None or pending_report_df.empty:
        st.success("Nenhuma pendência encontrada para esta competência.")
        return

    pending_buffer = dataframe_to_excel_download(pending_report_df, sheet_name="Pendencias Relatorio")
    st.download_button(
        "Exportar pendências em .xlsx",
        data=pending_buffer,
        file_name=f"pendencias_relatorio_cen_{sanitize_download_label(report_period)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
    )
    st.dataframe(
        format_machine_display_df(pending_report_df),
        use_container_width=True,
        height=320,
        hide_index=True,
    )


def prepare_rate_editor_df(df: pd.DataFrame, include_meta: bool = False) -> pd.DataFrame:
    columns = ["grupo", "modelo", "percentual", "ativo", "updated_at"]
    if include_meta:
        columns = ["grupo", "modelo", "percentual", "meta_margem", "ativo", "updated_at"]

    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    editor_df = df.copy()
    for column in columns:
        if column not in editor_df.columns:
            editor_df[column] = None
    return editor_df[columns].sort_values("modelo", na_position="last").reset_index(drop=True)


def prepare_manager_relation_editor_df(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "filial",
        "gerente",
        "cod_vendedor",
        "cod_x",
        "vendedor",
        "data_nascimento",
        "cpf",
        "email",
        "contato",
        "percentual_comissao_gerente",
        "ativo",
        "updated_at",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    editor_df = df.copy()
    for column in columns:
        if column not in editor_df.columns:
            editor_df[column] = None
    return editor_df[columns].sort_values(["filial", "gerente", "cod_vendedor"], na_position="last").reset_index(
        drop=True
    )


def _render_settings_view_legacy() -> None:
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
                count = replace_active_model_fat_rates(df_fat)
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
                count = replace_active_model_margin_rates(df_margin)
                st.success(f"{count} modelos salvos em comissao_margem_modelo.")
        except Exception as exc:
            st.error(f"Não foi possível salvar percentuais de margem: {exc}")


def render_settings_view() -> None:
    st.markdown('<div class="section-title">Configurações</div>', unsafe_allow_html=True)
    st.caption("Uploads e manutenção dos percentuais ativos de comissão por modelo.")

    col_schema, col_load = st.columns([1, 1])
    with col_schema:
        if st.button("Garantir tabelas Postgres", use_container_width=True):
            try:
                ensure_commission_tables()
                st.success("Tabelas Postgres prontas.")
            except Exception as exc:
                st.error(f"Não foi possível preparar as tabelas: {exc}")
    with col_load:
        if st.button("Carregar regras atuais", use_container_width=True):
            try:
                st.session_state.settings_fat_rates_df = prepare_rate_editor_df(read_model_fat_rates())
                st.session_state.settings_margin_rates_df = prepare_rate_editor_df(
                    read_model_margin_rates(),
                    include_meta=True,
                )
                st.session_state.settings_manager_relations_df = prepare_manager_relation_editor_df(
                    read_manager_relations()
                )
                st.success("Regras atuais carregadas do Postgres.")
            except Exception as exc:
                st.error(f"Não foi possível carregar as regras atuais: {exc}")

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
                count = replace_active_model_fat_rates(df_fat)
                st.session_state.settings_fat_rates_df = prepare_rate_editor_df(read_model_fat_rates())
                st.success(f"{count} modelos salvos em comissao_faturamento_modelo.")
        except Exception as exc:
            st.error(f"Não foi possível salvar percentuais de faturamento: {exc}")

    st.markdown("#### Regras atuais de faturamento")
    if st.session_state.get("settings_fat_rates_df") is None:
        try:
            st.session_state.settings_fat_rates_df = prepare_rate_editor_df(read_model_fat_rates())
        except Exception as exc:
            st.warning(f"Não foi possível carregar as regras de faturamento: {exc}")

    df_fat_current = st.session_state.get("settings_fat_rates_df")
    if df_fat_current is not None:
        edited_fat = st.data_editor(
            df_fat_current,
            use_container_width=True,
            height=320,
            num_rows="dynamic",
            disabled=["updated_at"],
            column_config={
                "grupo": st.column_config.TextColumn("Grupo"),
                "modelo": st.column_config.TextColumn("Modelo", required=True),
                "percentual": st.column_config.NumberColumn("% Comissão Fat.", format="%.4f"),
                "ativo": st.column_config.CheckboxColumn("Ativo", default=True),
                "updated_at": st.column_config.DatetimeColumn("Atualizado em"),
            },
            key="settings_fat_rates_editor",
        )
        if st.button("Salvar alterações de faturamento", type="primary", use_container_width=True):
            try:
                count = replace_active_model_fat_rates(edited_fat)
                st.session_state.settings_fat_rates_df = prepare_rate_editor_df(read_model_fat_rates())
                st.success(f"{count} regras de faturamento salvas como nova versão ativa.")
            except Exception as exc:
                st.error(f"Não foi possível salvar alterações de faturamento: {exc}")

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
                count = replace_active_model_margin_rates(df_margin)
                st.session_state.settings_margin_rates_df = prepare_rate_editor_df(
                    read_model_margin_rates(),
                    include_meta=True,
                )
                st.success(f"{count} modelos salvos em comissao_margem_modelo.")
        except Exception as exc:
            st.error(f"Não foi possível salvar percentuais de margem: {exc}")

    st.markdown("#### Regras atuais de margem")
    if st.session_state.get("settings_margin_rates_df") is None:
        try:
            st.session_state.settings_margin_rates_df = prepare_rate_editor_df(
                read_model_margin_rates(),
                include_meta=True,
            )
        except Exception as exc:
            st.warning(f"Não foi possível carregar as regras de margem: {exc}")

    df_margin_current = st.session_state.get("settings_margin_rates_df")
    if df_margin_current is not None:
        edited_margin = st.data_editor(
            df_margin_current,
            use_container_width=True,
            height=320,
            num_rows="dynamic",
            disabled=["updated_at"],
            column_config={
                "grupo": st.column_config.TextColumn("Grupo"),
                "modelo": st.column_config.TextColumn("Modelo", required=True),
                "percentual": st.column_config.NumberColumn("% Comissão Margem", format="%.4f"),
                "meta_margem": st.column_config.NumberColumn("Meta de Margem", format="%.4f"),
                "ativo": st.column_config.CheckboxColumn("Ativo", default=True),
                "updated_at": st.column_config.DatetimeColumn("Atualizado em"),
            },
            key="settings_margin_rates_editor",
        )
        if st.button("Salvar alterações de margem", type="primary", use_container_width=True):
            try:
                count = replace_active_model_margin_rates(edited_margin)
                st.session_state.settings_margin_rates_df = prepare_rate_editor_df(
                    read_model_margin_rates(),
                    include_meta=True,
                )
                st.success(f"{count} regras de margem salvas como nova versão ativa.")
            except Exception as exc:
                st.error(f"Não foi possível salvar alterações de margem: {exc}")
    st.divider()
    st.markdown("### Relação CEN x Gerente")
    manager_file = st.file_uploader(
        "Excel de CEN e gestores",
        type=["xlsx", "xls"],
        key="settings_manager_relation_upload",
    )
    if manager_file is not None:
        try:
            df_manager = pd.read_excel(manager_file)
            st.dataframe(df_manager.head(50), use_container_width=True, height=250)
            if st.button("Salvar relação CEN x Gerente", use_container_width=True):
                count = replace_active_manager_relations(df_manager)
                st.session_state.settings_manager_relations_df = prepare_manager_relation_editor_df(
                    read_manager_relations()
                )
                st.success(f"{count} vínculos salvos em comissao_gerente_vendedor.")
        except Exception as exc:
            st.error(f"Não foi possível salvar a relação CEN x Gerente: {exc}")

    st.markdown("#### Relações atuais de CEN x Gerente")
    if st.session_state.get("settings_manager_relations_df") is None:
        try:
            st.session_state.settings_manager_relations_df = prepare_manager_relation_editor_df(
                read_manager_relations()
            )
        except Exception as exc:
            st.warning(f"Não foi possível carregar as relações de gerente: {exc}")

    df_manager_current = st.session_state.get("settings_manager_relations_df")
    if df_manager_current is not None:
        edited_manager = st.data_editor(
            df_manager_current,
            use_container_width=True,
            height=360,
            num_rows="dynamic",
            disabled=["updated_at"],
            column_config={
                "filial": st.column_config.TextColumn("Filial"),
                "gerente": st.column_config.TextColumn("Gerente", required=True),
                "cod_vendedor": st.column_config.TextColumn("Cod Vendedor", required=True),
                "cod_x": st.column_config.TextColumn("Cod X"),
                "vendedor": st.column_config.TextColumn("Vendedor"),
                "data_nascimento": st.column_config.TextColumn("Data de Nascimento"),
                "cpf": st.column_config.TextColumn("CPF"),
                "email": st.column_config.TextColumn("E-mail"),
                "contato": st.column_config.TextColumn("Contato"),
                "percentual_comissao_gerente": st.column_config.NumberColumn("% ComissÃ£o Gerente", format="%.4f"),
                "ativo": st.column_config.CheckboxColumn("Ativo", default=True),
                "updated_at": st.column_config.DatetimeColumn("Atualizado em"),
            },
            key="settings_manager_relations_editor",
        )
        if st.button("Salvar alterações de CEN x Gerente", type="primary", use_container_width=True):
            try:
                count = replace_active_manager_relations(edited_manager)
                st.session_state.settings_manager_relations_df = prepare_manager_relation_editor_df(
                    read_manager_relations()
                )
                st.success(f"{count} relações de gerente salvas como nova versão ativa.")
            except Exception as exc:
                st.error(f"Não foi possível salvar alterações de CEN x Gerente: {exc}")


def main() -> None:
    load_dotenv()
    st.set_page_config(page_title="Apuração de Comissões", page_icon="✅", layout="wide")

    if "conn_ok" not in st.session_state:
        st.session_state.conn_ok = False
    if "results_df" not in st.session_state:
        st.session_state.results_df = None
    if "machine_df" not in st.session_state:
        st.session_state.machine_df = None
    if "machine_raw_df" not in st.session_state:
        st.session_state.machine_raw_df = None
    if "machine_full_history_df" not in st.session_state:
        st.session_state.machine_full_history_df = None
    if "machine_receivable_validation_df" not in st.session_state:
        st.session_state.machine_receivable_validation_df = None
    if "machine_paid_summary_df" not in st.session_state:
        st.session_state.machine_paid_summary_df = None
    if "machine_excluded_summary_df" not in st.session_state:
        st.session_state.machine_excluded_summary_df = None
    if "machine_pay_review_df" not in st.session_state:
        st.session_state.machine_pay_review_df = None
    if "machine_exclude_review_df" not in st.session_state:
        st.session_state.machine_exclude_review_df = None
    if "machine_selection_state" not in st.session_state:
        st.session_state.machine_selection_state = {}
    if "machine_rate_fat_df" not in st.session_state:
        st.session_state.machine_rate_fat_df = None
    if "machine_rate_margin_df" not in st.session_state:
        st.session_state.machine_rate_margin_df = None
    if "manager_relations_df" not in st.session_state:
        st.session_state.manager_relations_df = None
    if "machine_default_one_percent_enabled" not in st.session_state:
        st.session_state.machine_default_one_percent_enabled = True
    if "machine_default_one_percent_last" not in st.session_state:
        st.session_state.machine_default_one_percent_last = None
    if "settings_fat_rates_df" not in st.session_state:
        st.session_state.settings_fat_rates_df = None
    if "settings_margin_rates_df" not in st.session_state:
        st.session_state.settings_margin_rates_df = None
    if "settings_manager_relations_df" not in st.session_state:
        st.session_state.settings_manager_relations_df = None
    if "cen_report_df" not in st.session_state:
        st.session_state.cen_report_df = None
    if "cen_pending_report_df" not in st.session_state:
        st.session_state.cen_pending_report_df = None
    if "manager_report_df" not in st.session_state:
        st.session_state.manager_report_df = None
    if "coordinator_report_df" not in st.session_state:
        st.session_state.coordinator_report_df = None
    if "cen_report_period" not in st.session_state:
        st.session_state.cen_report_period = None
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
