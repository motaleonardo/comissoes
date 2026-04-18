import unittest
from datetime import date

import pandas as pd

from commission_tool.data.sources.postgres import _prepare_paid_commissions_df


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


if __name__ == "__main__":
    unittest.main()
