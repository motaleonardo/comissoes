import unittest

import pandas as pd

from commission_tool.core.apuracao import (
    apply_commission_rules,
    apply_frontend_default_fat_commission,
    apply_manager_commission_rules,
    apply_paid_history_filter,
)


class ApuracaoTests(unittest.TestCase):
    def test_applies_postgres_rates_by_model_and_recalculates_commissions(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Modelo": " 6125j ",
                    "Receita Bruta": 10000.0,
                    "% Margem Bruta": 15.0,
                    "% Comissao Fat.": 0.0,
                    "Valor Comissao Fat.": 0.0,
                    "Meta de Margem": 0.0,
                    "% Comissao Margem": 0.0,
                    "Valor Comissao Margem": 0.0,
                    "Valor Comissao Total": 0.0,
                },
                {
                    "Modelo": "SEM REGRA",
                    "Receita Bruta": 5000.0,
                    "% Margem Bruta": 5.0,
                    "% Comissao Fat.": 9.0,
                    "Valor Comissao Fat.": 450.0,
                    "Meta de Margem": 9.0,
                    "% Comissao Margem": 9.0,
                    "Valor Comissao Margem": 450.0,
                    "Valor Comissao Total": 900.0,
                },
            ]
        ).rename(
            columns={
                "% Comissao Fat.": "% Comissão Fat.",
                "Valor Comissao Fat.": "Valor Comissão Fat.",
                "% Comissao Margem": "% Comissão Margem",
                "Valor Comissao Margem": "Valor Comissão Margem",
                "Valor Comissao Total": "Valor Comissão Total",
            }
        )
        fat_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 5.0}])
        margin_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 1.25, "meta_margem": 12.5}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertEqual(result.loc[0, "% Comissão Fat."], 5.0)
        self.assertEqual(result.loc[0, "Valor Comissão Fat."], 500.0)
        self.assertEqual(result.loc[0, "Meta de Margem"], 12.5)
        self.assertEqual(result.loc[0, "% Comissão Margem"], 1.25)
        self.assertEqual(result.loc[0, "Valor Comissão Margem"], 125.0)
        self.assertEqual(result.loc[0, "Valor Comissão Total"], 625.0)
        self.assertTrue(result.loc[0, "Gatilho Comissão Margem Atingido"])
        self.assertTrue(result.loc[0, "Regra Comissão Fat. Encontrada"])
        self.assertTrue(result.loc[0, "Regra Comissão Margem Encontrada"])

        self.assertEqual(result.loc[1, "% Comissão Fat."], 0.0)
        self.assertEqual(result.loc[1, "Valor Comissão Total"], 0.0)
        self.assertFalse(result.loc[1, "Gatilho Comissão Margem Atingido"])
        self.assertFalse(result.loc[1, "Regra Comissão Fat. Encontrada"])
        self.assertFalse(result.loc[1, "Regra Comissão Margem Encontrada"])

    def test_classification_overrides_commission_for_implements_and_used_machines(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Implemento",
                    "Modelo": "IMP-A",
                    "Receita Bruta": 250000.0,
                    "% Margem Bruta": 40.0,
                },
                {
                    "Classificacao Venda": "Implemento",
                    "Modelo": "IMP-B",
                    "Receita Bruta": 150000.0,
                    "% Margem Bruta": 40.0,
                },
                {
                    "Classificacao Venda": "Maquinas JD - Usados",
                    "Modelo": "USED-A",
                    "Receita Bruta": 80000.0,
                    "% Margem Bruta": 40.0,
                },
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame(
            [
                {"modelo": "IMP-A", "percentual": 9.0},
                {"modelo": "IMP-B", "percentual": 9.0},
                {"modelo": "USED-A", "percentual": 9.0},
            ]
        )
        margin_rates = pd.DataFrame(
            [
                {"modelo": "IMP-A", "percentual": 2.0, "meta_margem": 10.0},
                {"modelo": "IMP-B", "percentual": 2.0, "meta_margem": 10.0},
                {"modelo": "USED-A", "percentual": 2.0, "meta_margem": 10.0},
            ]
        )

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertEqual(result.loc[0, "% Comissão Fat."], 0.84)
        self.assertEqual(result.loc[0, "Valor Comissão Total"], 2100.0)
        self.assertEqual(result.loc[1, "% Comissão Fat."], 1.0)
        self.assertEqual(result.loc[1, "Valor Comissão Total"], 1500.0)
        self.assertEqual(result.loc[2, "% Comissão Fat."], 1.0)
        self.assertEqual(result.loc[2, "Valor Comissão Total"], 800.0)
        self.assertEqual(result["% Comissão Margem"].tolist(), [0.0, 0.0, 0.0])
        self.assertEqual(result["Gatilho Comissão Margem Atingido"].tolist(), [False, False, False])

    def test_implemento_threshold_equal_to_200k_uses_lower_rate(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Implemento",
                    "Modelo": "IMP-C",
                    "Receita Bruta": 200000.0,
                    "% Margem Bruta": 40.0,
                },
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame([{"modelo": "IMP-C", "percentual": 9.0}])
        margin_rates = pd.DataFrame([{"modelo": "IMP-C", "percentual": 2.0, "meta_margem": 10.0}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertEqual(result.loc[0, "% Comissão Fat."], 0.84)
        self.assertEqual(result.loc[0, "Valor Comissão Total"], 1680.0)

    def test_normalizes_commission_percentages_that_were_imported_times_one_hundred(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Maquinas JD - Novos",
                    "Modelo": "S550",
                    "Receita Bruta": 100000.0,
                    "% Margem Bruta": 50.0,
                }
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame([{"modelo": "S550", "percentual": 45.0}])
        margin_rates = pd.DataFrame([{"modelo": "S550", "percentual": 0.0, "meta_margem": 0.0}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertEqual(result.loc[0, "% Comissão Fat."], 0.45)
        self.assertEqual(result.loc[0, "Valor Comissão Fat."], 450.0)

    def test_normalizes_margin_goal_from_legacy_integer_percentages(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Maquinas JD - Novos",
                    "Modelo": "S550",
                    "Receita Bruta": 100000.0,
                    "% Margem Bruta": 50.0,
                }
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame([{"modelo": "S550", "percentual": 0.45}])
        margin_rates = pd.DataFrame([{"modelo": "S550", "percentual": 20.0, "meta_margem": 45.0}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertEqual(result.loc[0, "% Comissão Margem"], 0.2)
        self.assertEqual(result.loc[0, "Meta de Margem"], 45.0)
        self.assertTrue(result.loc[0, "Gatilho Comissão Margem Atingido"])
        self.assertEqual(result.loc[0, "Valor Comissão Margem"], 200.0)

    def test_only_pays_margin_commission_when_margin_goal_is_met(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Maquinas JD - Novos",
                    "Modelo": "6125J",
                    "Receita Bruta": 100000.0,
                    "% Margem Bruta": 10.0,
                }
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 5.0}])
        margin_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 0.25, "meta_margem": 12.5}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertFalse(result.loc[0, "Gatilho Comissão Margem Atingido"])
        self.assertEqual(result.loc[0, "Valor Comissão Margem"], 0.0)
        self.assertEqual(result.loc[0, "Valor Comissão Total"], 5000.0)

    def test_pays_margin_commission_when_margin_goal_is_equal_to_margin_bruta(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Maquinas JD - Novos",
                    "Modelo": "6125J",
                    "Receita Bruta": 100000.0,
                    "% Margem Bruta": 12.5,
                }
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 5.0}])
        margin_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 0.25, "meta_margem": 12.5}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertTrue(result.loc[0, "Gatilho Comissão Margem Atingido"])
        self.assertEqual(result.loc[0, "Valor Comissão Margem"], 250.0)
        self.assertEqual(result.loc[0, "Valor Comissão Total"], 5250.0)

    def test_applies_manager_commission_from_seller_code(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Cod Vendedor": " a0000e ",
                    "Valor Comissao Total": 100.0,
                },
                {
                    "Cod Vendedor": "SEM_GERENTE",
                    "Valor Comissao Total": 80.0,
                },
            ]
        ).rename(columns={"Valor Comissao Total": "Valor Comissão Total"})
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A0000E",
                    "gerente": "Hernane Borges da Costa",
                    "percentual_comissao_gerente": 33.0,
                }
            ]
        )

        result = apply_manager_commission_rules(df_machine, manager_relations)

        self.assertEqual(result.loc[0, "Cod Vendedor"], "A0000E")
        self.assertEqual(result.loc[0, "Gerente"], "Hernane Borges da Costa")
        self.assertEqual(result.loc[0, "% Comissão Gerente"], 33.0)
        self.assertEqual(result.loc[0, "Valor Comissão Gerente"], 33.0)
        self.assertTrue(result.loc[0, "Regra Gerente Encontrada"])
        self.assertEqual(result.loc[1, "Gerente"], "")
        self.assertEqual(result.loc[1, "Valor Comissão Gerente"], 0.0)
        self.assertFalse(result.loc[1, "Regra Gerente Encontrada"])

    def test_applies_frontend_default_one_percent_only_when_enabled_and_rate_is_zero(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Receita Bruta": 10000.0,
                    "% Comissao Fat.": 0.0,
                    "Valor Comissao Fat.": 0.0,
                    "Valor Comissao Margem": 50.0,
                    "Valor Comissao Total": 50.0,
                },
                {
                    "Receita Bruta": 10000.0,
                    "% Comissao Fat.": 2.0,
                    "Valor Comissao Fat.": 200.0,
                    "Valor Comissao Margem": 20.0,
                    "Valor Comissao Total": 220.0,
                },
            ]
        ).rename(
            columns={
                "% Comissao Fat.": "% Comissão Fat.",
                "Valor Comissao Fat.": "Valor Comissão Fat.",
                "Valor Comissao Margem": "Valor Comissão Margem",
                "Valor Comissao Total": "Valor Comissão Total",
            }
        )

        result_enabled = apply_frontend_default_fat_commission(df_machine, enabled=True)
        result_disabled = apply_frontend_default_fat_commission(df_machine, enabled=False)

        self.assertEqual(result_enabled.loc[0, "% Comissão Fat."], 1.0)
        self.assertEqual(result_enabled.loc[0, "Valor Comissão Fat."], 100.0)
        self.assertEqual(result_enabled.loc[0, "Valor Comissão Total"], 150.0)
        self.assertTrue(result_enabled.loc[0, "Padrão 1% Fat. Aplicado"])
        self.assertEqual(result_enabled.loc[1, "% Comissão Fat."], 2.0)
        self.assertFalse(result_enabled.loc[1, "Padrão 1% Fat. Aplicado"])

        self.assertEqual(result_disabled.loc[0, "% Comissão Fat."], 0.0)
        self.assertEqual(result_disabled.loc[0, "Valor Comissão Total"], 50.0)
        self.assertFalse(result_disabled.loc[0, "Padrão 1% Fat. Aplicado"])

    def test_filters_positive_paid_history_and_keeps_negative_history_for_review(self):
        df_machine = pd.DataFrame(
            [
                {"Nro Chassi": "CH_POS", "Nro Documento": "1"},
                {"Nro Chassi": "CH_NEG", "Nro Documento": "2"},
                {"Nro Chassi": "CH_BOTH", "Nro Documento": "3"},
                {"Nro Chassi": "CH_NONE", "Nro Documento": "4"},
            ]
        )
        paid_summary = pd.DataFrame(
            [
                {
                    "nro_chassi": "CH_POS",
                    "qtd_lancamentos_pagos": 1,
                    "valor_pago_positivo": 100.0,
                    "valor_estornado_negativo": 0.0,
                    "saldo_comissao_paga_chassi": 100.0,
                    "tem_pagamento_positivo": True,
                    "tem_estorno_negativo": False,
                },
                {
                    "nro_chassi": "CH_NEG",
                    "qtd_lancamentos_pagos": 1,
                    "valor_pago_positivo": 0.0,
                    "valor_estornado_negativo": -100.0,
                    "saldo_comissao_paga_chassi": -100.0,
                    "tem_pagamento_positivo": False,
                    "tem_estorno_negativo": True,
                },
                {
                    "nro_chassi": "CH_BOTH",
                    "qtd_lancamentos_pagos": 2,
                    "valor_pago_positivo": 100.0,
                    "valor_estornado_negativo": -100.0,
                    "saldo_comissao_paga_chassi": 0.0,
                    "tem_pagamento_positivo": True,
                    "tem_estorno_negativo": True,
                },
            ]
        )

        candidates, full_history = apply_paid_history_filter(df_machine, paid_summary)

        self.assertEqual(candidates["Nro Chassi"].tolist(), ["CH_NEG", "CH_BOTH", "CH_NONE"])
        self.assertTrue(full_history.loc[0, "Bloqueado por Pagamento Histórico"])
        self.assertIn("Já pago", full_history.loc[0, "Status Confronto Pagas"])
        self.assertIn("estorno", full_history.loc[1, "Status Confronto Pagas"].lower())
        self.assertIn("estorno", full_history.loc[2, "Status Confronto Pagas"].lower())
        self.assertEqual(full_history.loc[3, "Status Confronto Pagas"], "Novo para apuração")

    def test_filters_excluded_chassis_even_without_paid_history(self):
        df_machine = pd.DataFrame(
            [
                {"Nro Chassi": "CH_KEEP", "Nro Documento": "1"},
                {"Nro Chassi": "CH_EXCLUDED", "Nro Documento": "2"},
            ]
        )
        excluded_summary = pd.DataFrame(
            [
                {
                    "nro_chassi": "CH_EXCLUDED",
                    "qtd_lancamentos_excluidos": 1,
                }
            ]
        )

        candidates, full_history = apply_paid_history_filter(
            df_machine,
            paid_summary=None,
            excluded_summary=excluded_summary,
        )

        self.assertEqual(candidates["Nro Chassi"].tolist(), ["CH_KEEP"])
        self.assertTrue(full_history.loc[1, "Bloqueado por Exclusão"])
        self.assertIn("excluído", full_history.loc[1, "Status Confronto Pagas"].lower())


if __name__ == "__main__":
    unittest.main()
