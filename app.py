# -*- coding: utf-8 -*-
import os
import io
import time
from datetime import datetime

import streamlit as st
import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# Classificações de venda que exigem validação de chassi (V1 - Incentivo)
CLASSIFICACOES_COM_CHASSI = {"Maquinas JD - Novos"}

st.set_page_config(
    page_title="Validador de Comissões",
    page_icon="✅",
    layout="wide"
)

# ─── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
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

    .metric-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        border-left: 5px solid #0d6e3f;
        margin-bottom: 0.5rem;
    }
    .metric-card.warn { border-left-color: #e67e22; }
    .metric-card.danger { border-left-color: #c0392b; }
    .metric-card h3 { margin: 0; font-size: 1.8rem; }
    .metric-card p  { margin: 0; font-size: 0.8rem; color: #666; }

    .status-apto    { color: #0d6e3f; font-weight: 700; }
    .status-nao     { color: #c0392b; font-weight: 700; }
    .status-na      { color: #7f8c8d; font-weight: 600; }

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
""", unsafe_allow_html=True)

# ─── HEADER ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>✅ Validador de Comissões</h1>
    <p>Grupo Luiz Hohl - Validação de Incentivos e Pagamento de Clientes</p>
</div>
""", unsafe_allow_html=True)

# ─── SESSION STATE ────────────────────────────────────────────────────────────
if "conn_ok" not in st.session_state:
    st.session_state.conn_ok = False
if "results_df" not in st.session_state:
    st.session_state.results_df = None


# ══════════════════════════════════════════════════════════════════════════════
#  FUNÇÕES DE CONEXÃO E VALIDAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

def get_connection(server, database, username, password, use_windows_auth):
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    trust_cert = os.getenv("DB_TRUST_SERVER_CERTIFICATE", "no")
    encrypt = os.getenv("DB_ENCRYPT", "yes")

    if use_windows_auth:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            f"TrustServerCertificate={trust_cert};Encrypt={encrypt};"
        )
    else:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password};"
            f"TrustServerCertificate={trust_cert};Encrypt={encrypt};"
        )
    return pyodbc.connect(conn_str, timeout=10)


def validar_incentivo(conn, chassi):
    """
    Validação 1 - Recebimento do Incentivo
    Chassi -> bdnIncentivos.[Nota Fiscal Número] -> bdnContasReceber.[Valor Saldo]
    """
    result = {
        "v1_status": "N/A",
        "v1_saldo": None,
        "v1_data_emissao": None,
        "v1_nf_incentivo": None,
        "v1_detalhe": "",
    }

    if not chassi or str(chassi).strip() in ("", "-", "nan"):
        result["v1_detalhe"] = "Chassi não informado"
        return result

    chassi_str = str(chassi).strip()

    try:
        cursor = conn.cursor()

        # Passo 1: busca NF na tabela de incentivos pelo chassi (trimmed no BD)
        cursor.execute(
            "SELECT TOP 1 [Nota Fiscal Número] FROM bdnIncentivos WHERE LTRIM(RTRIM([Chassi])) = ?",
            chassi_str
        )
        row = cursor.fetchone()

        if not row:
            result["v1_status"] = "NÃO ENCONTRADO"
            result["v1_detalhe"] = "Chassi não localizado em bdnIncentivos"
            return result

        nf_numero = str(row[0]).strip()
        result["v1_nf_incentivo"] = nf_numero

        # Passo 2: soma saldo em bdnContasReceber pelo título
        cursor.execute(
            """
            SELECT
                SUM([Valor Saldo]) AS saldo_total,
                MIN([Data de Emissão]) AS data_emissao
            FROM bdnContasReceber
            WHERE [Título Número] = ?
            """,
            nf_numero
        )
        row2 = cursor.fetchone()

        if row2 is None or row2[0] is None:
            result["v1_status"] = "NÃO ENCONTRADO"
            result["v1_detalhe"] = f"NF {nf_numero} não localizada em bdnContasReceber"
            return result

        saldo = float(row2[0])
        data_em = row2[1]
        result["v1_saldo"] = saldo
        result["v1_data_emissao"] = data_em

        if abs(saldo) < 0.01:
            result["v1_status"] = "APTO"
            result["v1_detalhe"] = "Incentivo recebido integralmente"
        else:
            result["v1_status"] = "NÃO APTO"
            result["v1_detalhe"] = f"Saldo pendente: R$ {saldo:,.2f}"

    except Exception as e:
        result["v1_status"] = "ERRO"
        result["v1_detalhe"] = str(e)

    return result


def validar_pagamento_cliente(conn, nro_documento, data_emissao_planilha):
    """
    Validação 2 - Pagamento do Cliente
    Nro Documento -> bdnFaturamento (confirma data) -> [Cliente Código]
    -> bdnContasReceber (cliente + título) -> soma [Valor Saldo] + [Tipo Título]
    """
    result = {
        "v2_status": "N/A",
        "v2_saldo": None,
        "v2_tipos_titulo": None,
        "v2_cliente_cod": None,
        "v2_detalhe": "",
    }

    if not nro_documento or str(nro_documento).strip() in ("", "-", "nan"):
        result["v2_detalhe"] = "Nro Documento não informado"
        return result

    # Normaliza: remove zeros à esquerda para comparar, mas mantém original
    doc_str = str(nro_documento).strip()
    # Se veio como float (ex: 46254.0), converte para inteiro string
    try:
        doc_str = str(int(float(doc_str)))
    except Exception:
        pass

    # Data da planilha para confirmação
    if pd.isna(data_emissao_planilha) or str(data_emissao_planilha) in ("NaT", ""):
        data_planilha = None
    else:
        data_planilha = pd.to_datetime(data_emissao_planilha).date()

    try:
        cursor = conn.cursor()

        # Passo 1: busca em bdnFaturamento - filtra por NF + Data para evitar colisão
        row = None
        for doc_variant in [doc_str, doc_str.zfill(9)]:
            # Tenta primeiro com filtro de data (se disponível)
            if data_planilha:
                data_str = data_planilha.strftime("%d/%m/%Y")
                cursor.execute(
                    """
                    SELECT TOP 1
                        [Cliente Código],
                        [Data de Emissão],
                        [Nota Fiscal Número]
                    FROM bdnFaturamento
                    WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?
                      AND [Data de Emissão] = ?
                    """,
                    (doc_variant, data_str)
                )
                row = cursor.fetchone()

            # Fallback sem data
            if not row:
                cursor.execute(
                    """
                    SELECT TOP 1
                        [Cliente Código],
                        [Data de Emissão],
                        [Nota Fiscal Número]
                    FROM bdnFaturamento
                    WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?
                    """,
                    (doc_variant,)
                )
                row = cursor.fetchone()

            if row:
                break

        if not row:
            result["v2_status"] = "NÃO ENCONTRADO"
            result["v2_detalhe"] = f"Documento {doc_str} não localizado em bdnFaturamento"
            return result

        cliente_cod = str(row[0]).strip()
        data_fat = row[1]
        nf_faturamento = str(row[2]).strip()
        result["v2_cliente_cod"] = cliente_cod

        # Confirmação da data de emissão
        if data_planilha and data_fat:
            data_fat_date = pd.to_datetime(data_fat).date() if not isinstance(data_fat, type(None)) else None
            if data_fat_date and data_fat_date != data_planilha:
                result["v2_status"] = "ATENÇÃO"
                result["v2_detalhe"] = (
                    f"Data divergente: planilha={data_planilha} | "
                    f"faturamento={data_fat_date}"
                )
                # Continua mesmo assim para trazer o saldo

        # Passo 2: busca saldo POR TIPO em bdnContasReceber por cliente + titulo
        cursor.execute(
            """
            SELECT [Tipo Título], SUM([Valor Saldo]) AS saldo_tipo
            FROM bdnContasReceber
            WHERE [Cliente Código] = ?
              AND [Título Número]  = ?
            GROUP BY [Tipo Título]
            """,
            (cliente_cod, nf_faturamento)
        )
        tipo_rows = cursor.fetchall()

        if not tipo_rows:
            result["v2_status"] = "NÃO ENCONTRADO"
            result["v2_detalhe"] = (
                f"Título {nf_faturamento} do cliente {cliente_cod} "
                "não localizado em bdnContasReceber"
            )
            return result

        # Calcula saldo total e identifica tipos com saldo pendente
        saldo_total = sum(float(r[1]) for r in tipo_rows if r[1] is not None)
        tipos_todos = [str(r[0]).strip() for r in tipo_rows if r[0]]
        tipos_com_saldo = [
            str(r[0]).strip().upper()
            for r in tipo_rows
            if r[1] is not None and abs(float(r[1])) >= 0.01
        ]

        result["v2_saldo"] = saldo_total
        result["v2_tipos_titulo"] = "/".join(tipos_todos)

        # Regra de aptidão
        if abs(saldo_total) < 0.01:
            result["v2_status"] = "APTO"
            result["v2_detalhe"] = "Cliente quitou integralmente"
        else:
            # Verifica se o saldo pendente vem APENAS de títulos tipo BL
            saldo_nao_bl = any(t != "BL" for t in tipos_com_saldo)

            if not saldo_nao_bl and tipos_com_saldo:
                result["v2_status"] = "APTO"
                result["v2_detalhe"] = (
                    f"Saldo R$ {saldo_total:,.2f} - pendente apenas em BL (apto conforme regra)"
                )
            else:
                tipos_pendentes = "/".join(tipos_com_saldo) if tipos_com_saldo else "N/A"
                result["v2_status"] = "NÃO APTO"
                result["v2_detalhe"] = (
                    f"Saldo pendente R$ {saldo_total:,.2f} - tipo(s) com saldo: {tipos_pendentes}"
                )

        # Preserva aviso de data se havia
        if result["v2_status"] == "ATENÇÃO":
            result["v2_status"] = "APTO*"
            result["v2_detalhe"] += " | Data divergente - verifique"

    except Exception as e:
        result["v2_status"] = "ERRO"
        result["v2_detalhe"] = str(e)

    return result


def _validar_pagamento_cliente_compat(conn, doc_str, data_planilha):
    """Versão compatível com SQL Server < 2017 (sem STRING_AGG)."""
    result = {
        "v2_status": "N/A",
        "v2_saldo": None,
        "v2_tipos_titulo": None,
        "v2_cliente_cod": None,
        "v2_detalhe": "",
    }
    try:
        cursor = conn.cursor()
        for variant in [doc_str, doc_str.zfill(9)]:
            cursor.execute(
                "SELECT TOP 1 [Cliente Código],[Data de Emissão],[Nota Fiscal Número] "
                "FROM bdnFaturamento WHERE [Nota Fiscal Número]=?", variant
            )
            row = cursor.fetchone()
            if row:
                break
        if not row:
            result["v2_status"] = "NÃO ENCONTRADO"
            result["v2_detalhe"] = f"Documento {doc_str} não encontrado"
            return result

        cliente_cod = str(row[0]).strip()
        nf_fat      = str(row[2]).strip()
        result["v2_cliente_cod"] = cliente_cod

        cursor.execute(
            "SELECT SUM([Valor Saldo]) FROM bdnContasReceber "
            "WHERE [Cliente Código]=? AND [Título Número]=?",
            cliente_cod, nf_fat
        )
        r2 = cursor.fetchone()
        saldo = float(r2[0]) if r2 and r2[0] is not None else None

        cursor.execute(
            "SELECT DISTINCT [Tipo Título] FROM bdnContasReceber "
            "WHERE [Cliente Código]=? AND [Título Número]=?",
            cliente_cod, nf_fat
        )
        tipos = "/".join([str(r[0]).strip() for r in cursor.fetchall() if r[0]])
        result["v2_saldo"]        = saldo
        result["v2_tipos_titulo"] = tipos

        if saldo is None:
            result["v2_status"]  = "NÃO ENCONTRADO"
            result["v2_detalhe"] = "Título não localizado em bdnContasReceber"
        elif abs(saldo) < 0.01:
            result["v2_status"]  = "APTO"
            result["v2_detalhe"] = "Cliente quitou integralmente"
        else:
            todos_bl = all(t.strip().upper() == "BL" for t in tipos.split("/") if t.strip())
            if todos_bl:
                result["v2_status"]  = "APTO"
                result["v2_detalhe"] = f"Saldo R$ {saldo:,.2f} - tipo BL (apto)"
            else:
                result["v2_status"]  = "NÃO APTO"
                result["v2_detalhe"] = f"Saldo pendente R$ {saldo:,.2f} - tipo(s): {tipos}"
    except Exception as e:
        result["v2_status"]  = "ERRO"
        result["v2_detalhe"] = str(e)
    return result


def carregar_planilha(uploaded_file):
    df = pd.read_excel(uploaded_file, sheet_name="Analitico CEN", header=23)
    # Remove linhas de totais/rodapé (sem Filial válida)
    df = df[df["Filial"].notna() & df["Filial"].astype(str).str.strip().ne("")]
    df = df[~df["Filial"].astype(str).str.strip().isin(["nan"])]
    df = df.reset_index(drop=True)

    # Normaliza Nro Documento -> 9 dígitos com zeros à esquerda
    def norm_doc(v):
        if pd.isna(v): return ""
        try:
            return str(int(float(str(v)))).zfill(9)
        except Exception:
            return str(v).strip()

    df["Nro Documento"] = df["Nro Documento"].apply(norm_doc)
    # Chassi -> preserva formato original do Excel, apenas strip de espaços
    df["Nro Chassi"] = df["Nro Chassi"].fillna("").astype(str).str.strip()
    return df


def diagnosticar_formatos(conn, df, max_amostras=20):
    """
    Diagnóstico de formato: verifica se os valores-chave da planilha
    existem no banco de dados, amostrando até max_amostras registros.
    """
    cursor = conn.cursor()
    resultados = []

    # Amostra representativa
    amostra = df.head(max_amostras)

    for _, row in amostra.iterrows():
        chassi = str(row.get("Nro Chassi", "")).strip()
        doc    = str(row.get("Nro Documento", "")).strip()
        classif = str(row.get("Classificação Venda", "")).strip()
        precisa_chassi = classif in CLASSIFICACOES_COM_CHASSI

        chassi_ok = False
        doc_ok    = False

        # Testa chassi (apenas para classificações que exigem)
        if precisa_chassi and chassi and chassi not in ("", "-", "NAN"):
            cursor.execute(
                "SELECT COUNT(*) FROM bdnIncentivos WHERE LTRIM(RTRIM([Chassi])) = ?",
                chassi
            )
            chassi_ok = cursor.fetchone()[0] > 0

        # Testa documento (com e sem zero-pad)
        if doc and doc not in ("", "-", "NAN"):
            cursor.execute(
                "SELECT COUNT(*) FROM bdnFaturamento WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?",
                doc
            )
            doc_ok = cursor.fetchone()[0] > 0

            if not doc_ok:
                doc_padded = doc.zfill(9)
                cursor.execute(
                    "SELECT COUNT(*) FROM bdnFaturamento WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?",
                    doc_padded
                )
                doc_ok = cursor.fetchone()[0] > 0

        resultados.append({
            "Filial":          row.get("Filial", ""),
            "Cliente":         str(row.get("Cliente", ""))[:40],
            "Classificação":   classif,
            "Nro Chassi":      chassi if chassi else "(vazio)",
            "Chassi no BD":    ("✅" if chassi_ok else "❌") if precisa_chassi else "⬜ N/A",
            "Nro Documento":   doc if doc else "(vazio)",
            "Documento no BD": "✅" if doc_ok else ("⬜" if not doc or doc in ("", "-", "NAN") else "❌"),
        })

    df_diag = pd.DataFrame(resultados)

    # Resumo
    chassi_testados = df_diag[~df_diag["Chassi no BD"].isin(["⬜ N/A"])]
    doc_testados    = df_diag[~df_diag["Documento no BD"].isin(["⬜"])]

    resumo = {
        "chassi_total":     len(chassi_testados),
        "chassi_ok":        (chassi_testados["Chassi no BD"] == "✅").sum() if len(chassi_testados) > 0 else 0,
        "doc_total":        len(doc_testados),
        "doc_ok":           (doc_testados["Documento no BD"] == "✅").sum() if len(doc_testados) > 0 else 0,
    }

    return df_diag, resumo


def executar_validacao(conn, df, progress_bar, status_text):
    results = []
    total = len(df)

    for i, row in df.iterrows():
        status_text.text(f"Validando linha {i+1} de {total} - {row.get('CEN','')[:40]}")
        progress_bar.progress((i + 1) / total)

        # Determina se precisa validar chassi pela classificação de venda
        classif = str(row.get("Classificação Venda", "")).strip()
        precisa_chassi = classif in CLASSIFICACOES_COM_CHASSI

        if precisa_chassi:
            v1 = validar_incentivo(conn, row["Nro Chassi"])
        else:
            v1 = {
                "v1_status": "N/A",
                "v1_saldo": None,
                "v1_data_emissao": None,
                "v1_nf_incentivo": None,
                "v1_detalhe": f"Classificação '{classif}' não requer validação de chassi",
            }
        v2 = validar_pagamento_cliente(conn, row["Nro Documento"], row["Data de Emissão"])

        # Status geral
        statuses = [v1["v1_status"], v2["v2_status"]]
        if "NÃO APTO" in statuses:
            status_geral = "❌ NÃO APTO"
        elif "ERRO" in statuses or "NÃO ENCONTRADO" in statuses:
            status_geral = "⚠️ VERIFICAR"
        elif all(s in ("APTO", "N/A", "APTO*") for s in statuses):
            status_geral = "✅ APTO"
        else:
            status_geral = "⚠️ VERIFICAR"

        results.append({
            "Filial":           row.get("Filial", ""),
            "CEN":              row.get("CEN", ""),
            "Cliente":          row.get("Cliente", ""),
            "Nro Documento":    row.get("Nro Documento", ""),
            "Nro Chassi":       row.get("Nro Chassi", ""),
            "Data de Emissão":  row.get("Data de Emissão", ""),
            "Valor Comissão":   row.get("Valor Comissão Total", 0),
            # Validação 1
            "V1 - Status":          v1["v1_status"],
            "V1 - NF Incentivo":    v1["v1_nf_incentivo"],
            "V1 - Saldo Incentivo": v1["v1_saldo"],
            "V1 - Data Emissão":    v1["v1_data_emissao"],
            "V1 - Detalhe":         v1["v1_detalhe"],
            # Validação 2
            "V2 - Status":          v2["v2_status"],
            "V2 - Cód. Cliente":    v2["v2_cliente_cod"],
            "V2 - Saldo Cliente":   v2["v2_saldo"],
            "V2 - Tipos Título":    v2["v2_tipos_titulo"],
            "V2 - Detalhe":         v2["v2_detalhe"],
            # Geral
            "Status Geral":         status_geral,
        })

    return pd.DataFrame(results)


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR - CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### ⚙️ Configuração do Banco")

    # Servidor e banco lidos do .env
    server   = os.getenv("DB_SERVER", "")
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
                st.session_state.conn_ok    = True
                st.session_state.conn_cfg   = dict(
                    server=server, database=database,
                    username=username, password=password,
                    use_windows_auth=use_windows
                )
                st.success("✅ Conexão estabelecida!")
            except Exception as e:
                st.session_state.conn_ok = False
                st.error(f"❌ Falha: {e}")

    st.divider()
    st.markdown("**Status da conexão:**")
    if st.session_state.conn_ok:
        st.success("🟢 Conectado")
    else:
        st.warning("🔴 Não conectado")

    st.divider()
    st.markdown("""
    **Tabelas esperadas no BD:**
    - `bdnIncentivos`
    - `bdnFaturamento`
    - `bdnContasReceber`
    """)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN - UPLOAD E EXECUÇÃO
# ══════════════════════════════════════════════════════════════════════════════

col_upload, col_info = st.columns([2, 1])

with col_upload:
    st.markdown('<div class="section-title">📂 Carregar Planilha de Comissões</div>', unsafe_allow_html=True)
    uploaded = st.file_uploader(
        "Selecione o arquivo Excel (aba: Analitico CEN)",
        type=["xlsx"],
        label_visibility="collapsed"
    )

with col_info:
    st.markdown('<div class="section-title">ℹ️ Regras aplicadas</div>', unsafe_allow_html=True)
    st.markdown("""
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
    """)

if uploaded:
    try:
        df_planilha = carregar_planilha(uploaded)
        st.success(f"✅ Planilha carregada - **{len(df_planilha)} registros** encontrados na aba 'Analitico CEN'")

        col_a, col_b = st.columns(2)
        col_a.metric("Total de registros", len(df_planilha))
        col_b.metric("Valor total comissão",
            f"R$ {pd.to_numeric(df_planilha['Valor Comissão Total'], errors='coerce').sum():,.2f}")

        with st.expander("👁️ Prévia da planilha"):
            preview_cols = [
                "Filial", "CEN", "Cliente", "Nro Documento", "Nro Chassi",
                "Data de Emissão",
                "Classificação Venda",
                "% Comissão NF", "Valor Comissão NF",
                "Meta Margem", "% Margem Bruta", "Valor Comissão Margem",
                "Valor Comissão Total",
            ]
            # Usa apenas colunas que existem na planilha
            preview_cols = [c for c in preview_cols if c in df_planilha.columns]
            st.dataframe(
                df_planilha[preview_cols],
                use_container_width=True, height=250
            )

        st.divider()

        if not st.session_state.conn_ok:
            st.warning("⚠️ Configure e teste a conexão com o banco de dados na barra lateral antes de executar.")
        else:
            # ── Diagnóstico de Dados ──────────────────────────────────────
            with st.expander("🔍 Diagnóstico de Dados - verificar formatos antes de validar"):
                if st.button("▶️ Executar Diagnóstico", use_container_width=True):
                    cfg = st.session_state.conn_cfg
                    try:
                        conn_diag = get_connection(**cfg)
                        with st.spinner("Verificando formatos..."):
                            df_diag, resumo = diagnosticar_formatos(conn_diag, df_planilha)
                        conn_diag.close()

                        # Resumo visual
                        cd1, cd2 = st.columns(2)
                        with cd1:
                            taxa_chassi = (resumo['chassi_ok'] / resumo['chassi_total'] * 100) if resumo['chassi_total'] > 0 else 0
                            cor = "normal" if taxa_chassi >= 80 else "off"
                            st.metric(
                                "Chassi encontrados no BD",
                                f"{resumo['chassi_ok']}/{resumo['chassi_total']}",
                                f"{taxa_chassi:.0f}%",
                                delta_color=cor,
                            )
                        with cd2:
                            taxa_doc = (resumo['doc_ok'] / resumo['doc_total'] * 100) if resumo['doc_total'] > 0 else 0
                            cor = "normal" if taxa_doc >= 80 else "off"
                            st.metric(
                                "Documentos encontrados no BD",
                                f"{resumo['doc_ok']}/{resumo['doc_total']}",
                                f"{taxa_doc:.0f}%",
                                delta_color=cor,
                            )

                        if taxa_chassi < 80 or taxa_doc < 80:
                            st.warning("⚠️ Taxa de correspondência baixa - verifique os formatos dos dados abaixo.")
                        else:
                            st.success("✅ Dados da planilha correspondem bem ao banco de dados.")

                        st.dataframe(df_diag, use_container_width=True, height=300)

                    except Exception as e:
                        st.error(f"Erro no diagnóstico: {e}")

            st.divider()

            if st.button("🚀 Executar Validação Completa", type="primary", use_container_width=True):
                cfg = st.session_state.conn_cfg
                try:
                    conn = get_connection(**cfg)

                    progress = st.progress(0)
                    status   = st.empty()
                    timer_placeholder = st.empty()

                    t0 = time.time()
                    with st.spinner("Validando comissões..."):
                        results_df = executar_validacao(conn, df_planilha, progress, status)
                    elapsed = time.time() - t0

                    conn.close()
                    progress.empty()
                    status.empty()
                    timer_placeholder.empty()
                    st.session_state.results_df = results_df

                    minutes, seconds = divmod(int(elapsed), 60)
                    st.success(f"✅ Validação concluída em {minutes}m {seconds}s!")

                except Exception as e:
                    st.error(f"Erro durante validação: {e}")

    except Exception as e:
        st.error(f"Erro ao ler planilha: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  RESULTADOS
# ══════════════════════════════════════════════════════════════════════════════

if st.session_state.results_df is not None:
    df_r = st.session_state.results_df
    # Limpa resultados antigos se colunas mudaram
    if "V1 - Status" not in df_r.columns:
        st.session_state.results_df = None
        st.warning("Resultados anteriores foram limpos (formato de colunas atualizado). Execute a validacao novamente.")
        st.stop()

    st.markdown('<div class="section-title">📊 Resultados da Validação</div>', unsafe_allow_html=True)

    # KPIs
    total  = len(df_r)
    aptos  = (df_r["Status Geral"] == "✅ APTO").sum()
    n_aptos = (df_r["Status Geral"] == "❌ NÃO APTO").sum()
    verif  = (df_r["Status Geral"] == "⚠️ VERIFICAR").sum()

    val_apta   = pd.to_numeric(df_r.loc[df_r["Status Geral"]=="✅ APTO","Valor Comissão"], errors="coerce").sum()
    val_n_apta = pd.to_numeric(df_r.loc[df_r["Status Geral"]=="❌ NÃO APTO","Valor Comissão"], errors="coerce").sum()

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total",         total)
    k2.metric("✅ Aptos",      aptos)
    k3.metric("❌ Não Aptos",  n_aptos)
    k4.metric("⚠️ Verificar",  verif)
    k5.metric("Valor Apto",    f"R$ {val_apta:,.2f}")
    k6.metric("Valor Bloqueado", f"R$ {val_n_apta:,.2f}")

    st.divider()

    # Filtros
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        filtro_status = st.multiselect(
            "Filtrar por Status Geral",
            options=df_r["Status Geral"].unique().tolist(),
            default=df_r["Status Geral"].unique().tolist()
        )
    with col_f2:
        filtro_filial = st.multiselect(
            "Filtrar por Filial",
            options=sorted(df_r["Filial"].dropna().unique().tolist()),
            default=[]
        )
    with col_f3:
        filtro_v1 = st.multiselect(
            "Status V1 (Incentivo)",
            options=df_r["V1 - Status"].unique().tolist(),
            default=[]
        )

    df_show = df_r[df_r["Status Geral"].isin(filtro_status)]
    if filtro_filial:
        df_show = df_show[df_show["Filial"].isin(filtro_filial)]
    if filtro_v1:
        df_show = df_show[df_show["V1 - Status"].isin(filtro_v1)]

    # Tabela de resultados
    display_cols = [
        "Status Geral",
        "Filial", "CEN", "Cliente",
        "Nro Documento", "Nro Chassi",
        "Data de Emissão", "Valor Comissão",
        "V1 - Status", "V1 - NF Incentivo", "V1 - Saldo Incentivo", "V1 - Detalhe",
        "V2 - Status", "V2 - Cód. Cliente", "V2 - Saldo Cliente",
        "V2 - Tipos Título", "V2 - Detalhe",
    ]

    st.dataframe(
        df_show[display_cols],
        use_container_width=True,
        height=450,
        column_config={
            "Status Geral":           st.column_config.TextColumn("Status", width=120),
            "Valor Comissão":         st.column_config.NumberColumn("Comissão R$", format="R$ %.2f"),
            "V1 - Saldo Incentivo":   st.column_config.NumberColumn("V1 Saldo", format="R$ %.2f"),
            "V2 - Saldo Cliente":     st.column_config.NumberColumn("V2 Saldo", format="R$ %.2f"),
        }
    )

    st.caption(f"Exibindo {len(df_show)} de {total} registros")

    # ── Exportar Excel ──────────────────────────────────────────────────────
    st.divider()
    st.markdown('<div class="section-title">📥 Exportar Resultado</div>', unsafe_allow_html=True)

    def to_excel_download(df):
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Resultado Validação")
            ws = writer.sheets["Resultado Validação"]
            # Largura automática básica
            for col_cells in ws.columns:
                max_len = max((len(str(c.value or "")) for c in col_cells), default=10)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
        buf.seek(0)
        return buf

    col_exp1, col_exp2 = st.columns(2)
    with col_exp1:
        buf_all = to_excel_download(df_r[display_cols])
        st.download_button(
            "⬇️ Exportar TODOS os resultados",
            data=buf_all,
            file_name=f"validacao_comissoes_{datetime.today().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    with col_exp2:
        df_nao_aptos = df_r[df_r["Status Geral"].isin(["❌ NÃO APTO", "⚠️ VERIFICAR"])]
        buf_nao = to_excel_download(df_nao_aptos[display_cols])
        st.download_button(
            "⬇️ Exportar apenas NÃO APTOS / VERIFICAR",
            data=buf_nao,
            file_name=f"pendencias_comissoes_{datetime.today().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
