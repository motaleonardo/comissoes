import unittest

import pandas as pd

from commission_tool.core.paid_audit import (
    FAT_RATE_PERCENT_COLUMNS,
    MARGIN_RATE_PERCENT_COLUMNS,
    build_rate_lookup,
    normalize_commission_report_df,
    validate_paid_commission_file,
)


class PaidAuditTests(unittest.TestCase):
    def test_normalizes_legacy_report_columns(self):
        df = pd.DataFrame(
            [
                {
                    "Filial": "Loja",
                    "Cliente": "Cliente A",
                    "% Comissão NF": "5,00%",
                    "Valor Comissão NF": "500,00",
                    "Meta Margem": "10,00%",
                }
            ]
        )

        normalized = normalize_commission_report_df(df)

        self.assertIn("Nome do Cliente", normalized.columns)
        self.assertIn("% Comissão Fat.", normalized.columns)
        self.assertIn("Valor Comissão Fat.", normalized.columns)
        self.assertIn("Meta de Margem", normalized.columns)

    def test_validates_commission_file_against_rules_and_extraction_keys(self):
        report = pd.DataFrame(
            [
                {
                    "Filial": "Loja",
                    "Modelo": "6125J",
                    "Nro Chassi": "CH1",
                    "Nro Documento": "123",
                    "Receita Bruta": "R$ 10.000,00",
                    "% Comissão NF": "5,00%",
                    "Valor Comissão NF": "R$ 500,00",
                    "% Comissão Margem": "1,00%",
                    "Valor Comissão Margem": "R$ 100,00",
                    "Valor Comissão Total": "R$ 600,00",
                }
            ]
        )
        fat_rules = build_rate_lookup(
            pd.DataFrame([{"Modelo": "6125J", "Percentual": "5,00%"}]),
            FAT_RATE_PERCENT_COLUMNS,
        )
        margin_rules = build_rate_lookup(
            pd.DataFrame([{"Modelo": "6125J", "Percentual": "1,00%"}]),
            MARGIN_RATE_PERCENT_COLUMNS,
        )

        result = validate_paid_commission_file(
            "arquivo.xlsx",
            report,
            fat_rules,
            margin_rules,
            {("CH1", "000000123")},
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(result.summary["used_6125j_percentages"], [5.0])
        self.assertEqual(result.summary["loaded_row_count"], 1)
        self.assertGreaterEqual(result.summary["loaded_column_count"], 9)
        self.assertEqual(result.summary["loaded_revenue"], 10000.0)
        self.assertEqual(result.summary["loaded_total_commission"], 600.0)

    def test_rejects_when_key_is_missing_from_current_extraction(self):
        report = pd.DataFrame(
            [
                {
                    "Filial": "Loja",
                    "Modelo": "6125J",
                    "Nro Chassi": "CH1",
                    "Nro Documento": "123",
                    "Receita Bruta": 10000,
                    "% Comissão Fat.": 5,
                    "Valor Comissão Fat.": 500,
                    "% Comissão Margem": 1,
                    "Valor Comissão Margem": 100,
                    "Valor Comissão Total": 600,
                }
            ]
        )

        result = validate_paid_commission_file(
            "arquivo.xlsx",
            report,
            {"6125J": {5.0}},
            {"6125J": {1.0}},
            {("OUTRO", "000000123")},
        )

        self.assertFalse(result.passed)
        self.assertIn("Chave na extração", result.issues["Regra"].tolist())


if __name__ == "__main__":
    unittest.main()
