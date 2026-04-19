import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from commission_tool.data.sources.postgres import (
    EXCLUDED_COMMISSIONS_TABLE,
    _prepare_model_fat_rates,
    _prepare_model_margin_rates,
    _prepare_paid_commissions_df,
    replace_active_model_fat_rates,
    replace_active_model_margin_rates,
    save_excluded_commissions,
)


class FakeConnection:
    def __init__(self):
        self.executed_sql = []

    def execute(self, statement, *args, **kwargs):
        self.executed_sql.append(str(statement))


class FakeEngine:
    def __init__(self, conn):
        self.conn = conn

    def begin(self):
        return self

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, traceback):
        return False


class PaidCommissionMappingTests(unittest.TestCase):
    def test_prepare_paid_commissions_maps_streamlit_columns(self):
        df = pd.DataFrame(
            [
                {
                    "Tipo": "Faturamento",
                    "Filial": "Loja A",
                    "Data de Emissão": "16/03/2026",
                    "Nro Documento": "0001",
                    "Modelo": "M1",
                    "Nro Chassi": "CH1",
                    "Valor Comissão Total": 123.45,
                }
            ]
        )

        prepared = _prepare_paid_commissions_df(
            df,
            competence_year=2026,
            competence_month=3,
            period_label="Março/2026",
            period_start=date(2026, 3, 16),
            period_end=date(2026, 4, 15),
            source="streamlit",
        )

        self.assertEqual(prepared.loc[0, "competencia_ano"], 2026)
        self.assertEqual(prepared.loc[0, "competencia_mes"], 3)
        self.assertEqual(prepared.loc[0, "tipo"], "Faturamento")
        self.assertEqual(prepared.loc[0, "nro_chassi"], "CH1")
        self.assertEqual(prepared.loc[0, "valor_comissao_total"], 123.45)

    def test_prepare_paid_commissions_parses_brazilian_money_and_excel_percentages(self):
        df = pd.DataFrame(
            [
                {
                    "Data de Emiss\u00e3o": "16/03/2026",
                    "Receita Bruta": "R$ 15.000,00",
                    "% Comiss\u00e3o Fat.": 0.05,
                    "% Margem Direta": "22,50%",
                    "Valor Comiss\u00e3o Total": "R$ 750,00",
                }
            ]
        )

        prepared = _prepare_paid_commissions_df(
            df,
            competence_year=2026,
            competence_month=3,
            period_label="Marco/2026",
            period_start=date(2026, 3, 16),
            period_end=date(2026, 4, 15),
            source="upload_excel",
        )

        self.assertEqual(prepared.loc[0, "receita_bruta"], 15000.0)
        self.assertEqual(prepared.loc[0, "perc_comissao_fat"], 5.0)
        self.assertEqual(prepared.loc[0, "perc_margem_direta"], 22.5)
        self.assertEqual(prepared.loc[0, "valor_comissao_total"], 750.0)

    def test_model_commission_percentages_do_not_multiply_decimal_inputs_by_one_hundred(self):
        fat_prepared = _prepare_model_fat_rates(
            pd.DataFrame([{"Modelo": "S550", "Percentual": 0.45}])
        )
        margin_prepared = _prepare_model_margin_rates(
            pd.DataFrame([{"Modelo": "S550", "Percentual": 0.2, "Meta de Margem": 0.45}])
        )

        self.assertEqual(fat_prepared.loc[0, "percentual"], 0.45)
        self.assertEqual(margin_prepared.loc[0, "percentual"], 0.2)
        self.assertEqual(margin_prepared.loc[0, "meta_margem"], 0.45)

    def test_model_rule_upload_normalizes_legacy_integer_percentages(self):
        fat_prepared = _prepare_model_fat_rates(
            pd.DataFrame([{"Modelo": "S550", "Percentual": 45.0}])
        )
        margin_prepared = _prepare_model_margin_rates(
            pd.DataFrame([{"Modelo": "S550", "Percentual": 20.0, "Meta de Margem": 45.0}])
        )

        self.assertEqual(fat_prepared.loc[0, "percentual"], 0.45)
        self.assertEqual(margin_prepared.loc[0, "percentual"], 0.2)
        self.assertEqual(margin_prepared.loc[0, "meta_margem"], 0.45)

    def test_replace_active_fat_rates_deactivates_current_rows_and_appends_new_version(self):
        fake_conn = FakeConnection()
        captured = {}

        def fake_to_sql(self, table_name, conn, if_exists, index):
            captured["table_name"] = table_name
            captured["if_exists"] = if_exists
            captured["index"] = index
            captured["df"] = self.copy()

        with (
            patch("commission_tool.data.sources.postgres.ensure_commission_tables"),
            patch("commission_tool.data.sources.postgres.get_engine", return_value=FakeEngine(fake_conn)),
            patch.object(pd.DataFrame, "to_sql", fake_to_sql),
        ):
            count = replace_active_model_fat_rates(
                pd.DataFrame([{"Modelo": "6125J", "Percentual": "5,00%"}])
            )

        self.assertEqual(count, 1)
        self.assertTrue(any("UPDATE comissao_faturamento_modelo" in sql for sql in fake_conn.executed_sql))
        self.assertTrue(any("ativo = FALSE" in sql for sql in fake_conn.executed_sql))
        self.assertFalse(any("DELETE FROM comissao_faturamento_modelo" in sql for sql in fake_conn.executed_sql))
        self.assertEqual(captured["table_name"], "comissao_faturamento_modelo")
        self.assertEqual(captured["if_exists"], "append")
        self.assertEqual(captured["df"].loc[0, "percentual"], 5.0)
        self.assertTrue(captured["df"].loc[0, "ativo"])

    def test_replace_active_margin_rates_deactivates_current_rows_and_appends_new_version(self):
        fake_conn = FakeConnection()
        captured = {}

        def fake_to_sql(self, table_name, conn, if_exists, index):
            captured["table_name"] = table_name
            captured["df"] = self.copy()

        with (
            patch("commission_tool.data.sources.postgres.ensure_commission_tables"),
            patch("commission_tool.data.sources.postgres.get_engine", return_value=FakeEngine(fake_conn)),
            patch.object(pd.DataFrame, "to_sql", fake_to_sql),
        ):
            count = replace_active_model_margin_rates(
                pd.DataFrame([{"Modelo": "6125J", "Percentual": "1,00%", "Meta de Margem": "12,50%"}])
            )

        self.assertEqual(count, 1)
        self.assertTrue(any("UPDATE comissao_margem_modelo" in sql for sql in fake_conn.executed_sql))
        self.assertFalse(any("DELETE FROM comissao_margem_modelo" in sql for sql in fake_conn.executed_sql))
        self.assertEqual(captured["table_name"], "comissao_margem_modelo")
        self.assertEqual(captured["df"].loc[0, "percentual"], 1.0)
        self.assertEqual(captured["df"].loc[0, "meta_margem"], 0.125)
        self.assertTrue(captured["df"].loc[0, "ativo"])

    def test_save_excluded_commissions_uses_excluded_table_with_paid_schema(self):
        fake_conn = FakeConnection()
        captured = {}

        def fake_to_sql(self, table_name, conn, if_exists, index):
            captured["table_name"] = table_name
            captured["if_exists"] = if_exists
            captured["df"] = self.copy()

        with (
            patch("commission_tool.data.sources.postgres.ensure_commission_tables"),
            patch("commission_tool.data.sources.postgres.get_engine", return_value=FakeEngine(fake_conn)),
            patch.object(pd.DataFrame, "to_sql", fake_to_sql),
        ):
            count = save_excluded_commissions(
                pd.DataFrame(
                    [
                        {
                            "Tipo": "Faturamento",
                            "Nro Chassi": "CH_EXCLUDED",
                            "Valor Comissão Total": 123.45,
                        }
                    ]
                ),
                competence_year=2026,
                competence_month=3,
                period_label="Março/2026",
                source="streamlit_exclusao",
            )

        self.assertEqual(count, 1)
        self.assertEqual(captured["table_name"], EXCLUDED_COMMISSIONS_TABLE)
        self.assertEqual(captured["if_exists"], "append")
        self.assertEqual(captured["df"].loc[0, "nro_chassi"], "CH_EXCLUDED")
        self.assertEqual(captured["df"].loc[0, "valor_comissao_total"], 123.45)


if __name__ == "__main__":
    unittest.main()
