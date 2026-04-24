import unittest

import pandas as pd

from commission_tool.core.paid_audit import (
    FAT_RATE_PERCENT_COLUMNS,
    MARGIN_RATE_PERCENT_COLUMNS,
    build_margin_rule_lookup,
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
                    "% Comissao NF": "5,00%",
                    "Valor Comissao NF": "500,00",
                    "Meta Margem": "10,00%",
                }
            ]
        ).rename(
            columns={
                "% Comissao NF": "% Comissão NF",
                "Valor Comissao NF": "Valor Comissão NF",
            }
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
                    "Meta Margem": "12,50%",
                    "% Margem Bruta": "15,00%",
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
        margin_rules = build_margin_rule_lookup(
            pd.DataFrame([{"Modelo": "6125J", "Percentual": "1,00%", "Meta": 0.125}])
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
        self.assertGreaterEqual(result.summary["loaded_column_count"], 11)
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
                    "Meta de Margem": 12.5,
                    "% Margem Bruta": 15.0,
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
            {"6125J": {"percentuais": {1.0}, "metas": {12.5}}},
            {("OUTRO", "000000123")},
        )

        self.assertFalse(result.passed)
        self.assertIn("Chave na extração", result.issues["Regra"].tolist())

    def test_rejects_margin_commission_when_margin_goal_is_not_met(self):
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
                    "Meta de Margem": 12.5,
                    "% Margem Bruta": 10.0,
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
            {"6125J": {"percentuais": {1.0}, "metas": {12.5}}},
            {("CH1", "000000123")},
        )

        self.assertFalse(result.passed)
        self.assertIn("Gatilho Comissão Margem", result.issues["Regra"].tolist())
        self.assertIn("Valor Comissão Total", result.issues["Regra"].tolist())


if __name__ == "__main__":
    unittest.main()
