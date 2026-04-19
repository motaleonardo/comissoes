"""Eligibility validation rules for commission payment."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from commission_tool.config import (
    CLASSIFICACOES_COM_CHASSI,
    STATUS_APTO,
    STATUS_NAO_APTO,
    STATUS_VERIFICAR,
)
from commission_tool.data.sources.sqlserver import SQLServerDataSource


def normalize_document(value: Any) -> str:
    if pd.isna(value):
        return ""
    try:
        return str(int(float(str(value))))
    except Exception:
        return str(value).strip()


def normalize_document_padded(value: Any) -> str:
    doc = normalize_document(value)
    return doc.zfill(9) if doc else ""


def parse_optional_date(value: Any):
    if pd.isna(value) or str(value) in ("NaT", ""):
        return None
    return pd.to_datetime(value).date()


def is_blank_key(value: Any) -> bool:
    return not value or str(value).strip().lower() in ("", "-", "nan")


def validate_incentive(source: SQLServerDataSource, chassi: Any) -> dict[str, Any]:
    result = {
        "v1_status": "N/A",
        "v1_saldo": None,
        "v1_data_emissao": None,
        "v1_nf_incentivo": None,
        "v1_detalhe": "",
    }

    if is_blank_key(chassi):
        result["v1_detalhe"] = "Chassi não informado"
        return result

    chassi_str = str(chassi).strip()

    try:
        nf_numero = source.find_incentive_invoice_by_chassi(chassi_str)
        if not nf_numero:
            result["v1_status"] = "NÃO ENCONTRADO"
            result["v1_detalhe"] = "Chassi não localizado em bdnIncentivos"
            return result

        result["v1_nf_incentivo"] = nf_numero
        summary = source.get_receivable_summary_by_title(nf_numero)
        if summary is None:
            result["v1_status"] = "NÃO ENCONTRADO"
            result["v1_detalhe"] = f"NF {nf_numero} não localizada em bdnContasReceber"
            return result

        result["v1_saldo"] = summary.saldo_total
        result["v1_data_emissao"] = summary.data_emissao

        if abs(summary.saldo_total) < 0.01:
            result["v1_status"] = "APTO"
            result["v1_detalhe"] = "Incentivo recebido integralmente"
        else:
            result["v1_status"] = "NÃO APTO"
            result["v1_detalhe"] = f"Saldo pendente: R$ {summary.saldo_total:,.2f}"
    except Exception as exc:
        result["v1_status"] = "ERRO"
        result["v1_detalhe"] = str(exc)

    return result


def validate_customer_payment(
    source: SQLServerDataSource,
    nro_documento: Any,
    data_emissao_planilha: Any,
) -> dict[str, Any]:
    result = {
        "v2_status": "N/A",
        "v2_saldo": None,
        "v2_tipos_titulo": None,
        "v2_cliente_cod": None,
        "v2_detalhe": "",
    }

    if is_blank_key(nro_documento):
        result["v2_detalhe"] = "Nro Documento não informado"
        return result

    doc_str = normalize_document(nro_documento)
    data_planilha = parse_optional_date(data_emissao_planilha)
    data_warning = ""

    try:
        invoice = source.find_invoice([doc_str, doc_str.zfill(9)], data_planilha)
        if invoice is None:
            result["v2_status"] = "NÃO ENCONTRADO"
            result["v2_detalhe"] = f"Documento {doc_str} não localizado em bdnFaturamento"
            return result

        result["v2_cliente_cod"] = invoice.cliente_codigo

        if data_planilha and invoice.data_emissao:
            data_fat = pd.to_datetime(invoice.data_emissao).date()
            if data_fat != data_planilha:
                data_warning = (
                    f"Data divergente: planilha={data_planilha} | "
                    f"faturamento={data_fat}"
                )

        receivables = source.get_receivables_by_customer_title(
            invoice.cliente_codigo,
            invoice.nota_fiscal_numero,
        )
        if not receivables:
            result["v2_status"] = "NÃO ENCONTRADO"
            result["v2_detalhe"] = (
                f"Título {invoice.nota_fiscal_numero} do cliente {invoice.cliente_codigo} "
                "não localizado em bdnContasReceber"
            )
            return result

        saldo_total = sum(item.saldo for item in receivables)
        tipos_todos = [item.tipo_titulo for item in receivables if item.tipo_titulo]
        tipos_com_saldo = [
            item.tipo_titulo.upper()
            for item in receivables
            if item.tipo_titulo and abs(item.saldo) >= 0.01
        ]

        result["v2_saldo"] = saldo_total
        result["v2_tipos_titulo"] = "/".join(tipos_todos)

        if abs(saldo_total) < 0.01:
            result["v2_status"] = "APTO"
            result["v2_detalhe"] = "Cliente quitou integralmente"
        elif tipos_com_saldo and all(tipo == "BL" for tipo in tipos_com_saldo):
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

        if data_warning:
            result["v2_status"] = "ATENÇÃO" if result["v2_status"] == "APTO" else result["v2_status"]
            result["v2_detalhe"] = f"{result['v2_detalhe']} | {data_warning}"
    except Exception as exc:
        result["v2_status"] = "ERRO"
        result["v2_detalhe"] = str(exc)

    return result


def combine_eligibility_status(v1_status: str, v2_status: str) -> str:
    statuses = [v1_status, v2_status]
    if "NÃO APTO" in statuses:
        return STATUS_NAO_APTO
    if any(status in ("ERRO", "NÃO ENCONTRADO", "ATENÇÃO") for status in statuses):
        return STATUS_VERIFICAR
    if all(status in ("APTO", "N/A") for status in statuses):
        return STATUS_APTO
    return STATUS_VERIFICAR


def run_eligibility_validation(
    conn,
    df: pd.DataFrame,
    progress_callback: Callable[[int, int, pd.Series], None] | None = None,
) -> pd.DataFrame:
    source = SQLServerDataSource(conn)
    results = []
    total = len(df)

    for i, row in df.iterrows():
        if progress_callback:
            progress_callback(i + 1, total, row)

        classif = str(row.get("Classificação Venda", "")).strip()
        if classif in CLASSIFICACOES_COM_CHASSI:
            v1 = validate_incentive(source, row.get("Nro Chassi", ""))
        else:
            v1 = {
                "v1_status": "N/A",
                "v1_saldo": None,
                "v1_data_emissao": None,
                "v1_nf_incentivo": None,
                "v1_detalhe": f"Classificação '{classif}' não requer validação de chassi",
            }

        v2 = validate_customer_payment(
            source,
            row.get("Nro Documento", ""),
            row.get("Data de Emissão", ""),
        )
        status_geral = combine_eligibility_status(v1["v1_status"], v2["v2_status"])

        result_row = {
            "Filial": row.get("Filial", ""),
            "CEN": row.get("CEN", ""),
            "Cliente": row.get("Cliente", ""),
            "Nro Documento": row.get("Nro Documento", ""),
            "Nro Chassi": row.get("Nro Chassi", ""),
            "Data de Emissão": row.get("Data de Emissão", ""),
            "Valor Comissão": row.get("Valor Comissão Total", 0),
            "V1 - Status": v1["v1_status"],
            "V1 - NF Incentivo": v1["v1_nf_incentivo"],
            "V1 - Saldo Incentivo": v1["v1_saldo"],
            "V1 - Data Emissão": v1["v1_data_emissao"],
            "V1 - Detalhe": v1["v1_detalhe"],
            "V2 - Status": v2["v2_status"],
            "V2 - Cód. Cliente": v2["v2_cliente_cod"],
            "V2 - Saldo Cliente": v2["v2_saldo"],
            "V2 - Tipos Título": v2["v2_tipos_titulo"],
            "V2 - Detalhe": v2["v2_detalhe"],
            "Status Geral": status_geral,
        }
        if "__apuracao_row_id" in row:
            result_row["__apuracao_row_id"] = row.get("__apuracao_row_id")
        results.append(result_row)

    return pd.DataFrame(results)


def diagnose_key_formats(conn, df: pd.DataFrame, max_samples: int = 20):
    source = SQLServerDataSource(conn)
    rows = []

    for _, row in df.head(max_samples).iterrows():
        chassi = str(row.get("Nro Chassi", "")).strip()
        doc = str(row.get("Nro Documento", "")).strip()
        classif = str(row.get("Classificação Venda", "")).strip()
        needs_chassi = classif in CLASSIFICACOES_COM_CHASSI

        chassi_ok = False
        doc_ok = False

        if needs_chassi and chassi and chassi.upper() not in ("", "-", "NAN"):
            chassi_ok = source.count_chassi(chassi) > 0

        if doc and doc.upper() not in ("", "-", "NAN"):
            doc_ok = source.count_document(doc) > 0
            if not doc_ok:
                doc_ok = source.count_document(doc.zfill(9)) > 0

        rows.append(
            {
                "Filial": row.get("Filial", ""),
                "Cliente": str(row.get("Cliente", ""))[:40],
                "Classificação": classif,
                "Nro Chassi": chassi if chassi else "(vazio)",
                "Chassi no BD": ("✅" if chassi_ok else "❌") if needs_chassi else "⬜ N/A",
                "Nro Documento": doc if doc else "(vazio)",
                "Documento no BD": "✅" if doc_ok else ("⬜" if not doc or doc.upper() in ("", "-", "NAN") else "❌"),
            }
        )

    df_diag = pd.DataFrame(rows)
    chassi_testados = df_diag[~df_diag["Chassi no BD"].isin(["⬜ N/A"])]
    doc_testados = df_diag[~df_diag["Documento no BD"].isin(["⬜"])]

    summary = {
        "chassi_total": len(chassi_testados),
        "chassi_ok": int((chassi_testados["Chassi no BD"] == "✅").sum())
        if len(chassi_testados) > 0
        else 0,
        "doc_total": len(doc_testados),
        "doc_ok": int((doc_testados["Documento no BD"] == "✅").sum())
        if len(doc_testados) > 0
        else 0,
    }

    return df_diag, summary
