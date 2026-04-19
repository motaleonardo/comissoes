import unittest
from datetime import date

from commission_tool.config import STATUS_NAO_APTO, STATUS_VERIFICAR
from commission_tool.core.eligibility import (
    combine_eligibility_status,
    run_eligibility_validation,
    validate_customer_payment,
    validate_incentive,
)
from commission_tool.data.sources.sqlserver import (
    FaturamentoRecord,
    ReceivableByType,
    ReceivableSummary,
)


class FakeSource:
    def __init__(self):
        self.incentive_nf = {}
        self.title_summary = {}
        self.invoices = {}
        self.receivables = {}

    def find_incentive_invoice_by_chassi(self, chassi):
        return self.incentive_nf.get(chassi)

    def get_receivable_summary_by_title(self, titulo):
        return self.title_summary.get(titulo)

    def find_invoice(self, document_variants, issue_date=None):
        for document in document_variants:
            if document in self.invoices:
                return self.invoices[document]
        return None

    def get_receivables_by_customer_title(self, cliente_codigo, titulo):
        return self.receivables.get((cliente_codigo, titulo), [])


class EligibilityTests(unittest.TestCase):
    def test_incentive_is_apto_when_receivable_balance_is_zero(self):
        source = FakeSource()
        source.incentive_nf["CHASSI1"] = "NF123"
        source.title_summary["NF123"] = ReceivableSummary(saldo_total=0.0, data_emissao=date(2026, 4, 1))

        result = validate_incentive(source, "CHASSI1")

        self.assertEqual(result["v1_status"], "APTO")
        self.assertEqual(result["v1_nf_incentivo"], "NF123")

    def test_customer_payment_is_apto_when_only_bl_has_balance(self):
        source = FakeSource()
        source.invoices["46254"] = FaturamentoRecord(
            cliente_codigo="CLI1",
            data_emissao=date(2026, 4, 1),
            nota_fiscal_numero="46254",
        )
        source.receivables[("CLI1", "46254")] = [ReceivableByType(tipo_titulo="BL", saldo=150.0)]

        result = validate_customer_payment(source, "46254", date(2026, 4, 1))

        self.assertEqual(result["v2_status"], "APTO")
        self.assertIn("BL", result["v2_tipos_titulo"])

    def test_customer_payment_is_not_apto_when_non_bl_has_balance(self):
        source = FakeSource()
        source.invoices["46254"] = FaturamentoRecord(
            cliente_codigo="CLI1",
            data_emissao=date(2026, 4, 1),
            nota_fiscal_numero="46254",
        )
        source.receivables[("CLI1", "46254")] = [ReceivableByType(tipo_titulo="DP", saldo=150.0)]

        result = validate_customer_payment(source, "46254", date(2026, 4, 1))

        self.assertEqual(result["v2_status"], "NÃO APTO")
        self.assertEqual(combine_eligibility_status("N/A", result["v2_status"]), STATUS_NAO_APTO)

    def test_date_divergence_moves_general_status_to_verify(self):
        source = FakeSource()
        source.invoices["46254"] = FaturamentoRecord(
            cliente_codigo="CLI1",
            data_emissao=date(2026, 4, 2),
            nota_fiscal_numero="46254",
        )
        source.receivables[("CLI1", "46254")] = [ReceivableByType(tipo_titulo="BL", saldo=0.0)]

        result = validate_customer_payment(source, "46254", date(2026, 4, 1))

        self.assertEqual(result["v2_status"], "ATENÇÃO")
        self.assertEqual(combine_eligibility_status("N/A", result["v2_status"]), STATUS_VERIFICAR)


    def test_run_validation_preserves_apuracao_row_id_when_present(self):
        import pandas as pd
        from unittest.mock import patch

        source = FakeSource()
        source.invoices["123"] = FaturamentoRecord(
            cliente_codigo="CLI1",
            data_emissao=date(2026, 4, 1),
            nota_fiscal_numero="123",
        )
        source.receivables[("CLI1", "123")] = [ReceivableByType(tipo_titulo="BL", saldo=0.0)]
        df = pd.DataFrame(
            [
                {
                    "__apuracao_row_id": 42,
                    "Classificação Venda": "Implemento",
                    "Nro Documento": "123",
                    "Data de Emissão": date(2026, 4, 1),
                }
            ]
        )

        with patch("commission_tool.core.eligibility.SQLServerDataSource", return_value=source):
            result = run_eligibility_validation(object(), df)

        self.assertIn("__apuracao_row_id", result.columns)
        self.assertEqual(result.loc[0, "__apuracao_row_id"], 42)


if __name__ == "__main__":
    unittest.main()
