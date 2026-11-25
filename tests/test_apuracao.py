import unittest

import pandas as pd

from commission_tool.core.apuracao import (
    apply_commission_rules,
    apply_manager_commission_rules,
    apply_paid_history_filter,
    filter_old_emissions,
    find_pending_registrations,
    prepare_machine_apuracao,
    PENDING_CEN_COLUMNS,
    PENDING_MODEL_COLUMNS,
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
        paid_document_summary = pd.DataFrame(
            [
                {
                    "nro_chassi": "CH_POS",
                    "nro_documento": "1",
                    "qtd_lancamentos_documento_pago": 1,
                    "valor_comissao_total_documento": 100.0,
                }
            ]
        )

        candidates, full_history = apply_paid_history_filter(
            df_machine,
            paid_summary,
            paid_document_summary=paid_document_summary,
        )

        self.assertEqual(candidates["Nro Chassi"].tolist(), ["CH_NEG", "CH_BOTH", "CH_NONE"])
        self.assertTrue(full_history.loc[0, "Bloqueado por Pagamento Histórico"])
        self.assertIn("documento", full_history.loc[0, "Status Confronto Pagas"].lower())
        self.assertIn("estorno", full_history.loc[1, "Status Confronto Pagas"].lower())
        self.assertIn("estorno", full_history.loc[2, "Status Confronto Pagas"].lower())
        self.assertEqual(full_history.loc[3, "Status Confronto Pagas"], "Novo para apuração")

    def test_keeps_new_document_for_same_chassi_as_candidate_and_blocks_only_exact_pair(self):
        df_machine = pd.DataFrame(
            [
                {"Nro Chassi": "CH_REPEAT", "Nro Documento": "000123"},
                {"Nro Chassi": "CH_REPEAT", "Nro Documento": "000124"},
            ]
        )
        paid_summary = pd.DataFrame(
            [
                {
                    "nro_chassi": "CH_REPEAT",
                    "qtd_lancamentos_pagos": 1,
                    "valor_pago_positivo": 500.0,
                    "valor_estornado_negativo": 0.0,
                    "saldo_comissao_paga_chassi": 500.0,
                    "tem_pagamento_positivo": True,
                    "tem_estorno_negativo": False,
                }
            ]
        )
        paid_document_summary = pd.DataFrame(
            [
                {
                    "nro_chassi": "CH_REPEAT",
                    "nro_documento": "123",
                    "qtd_lancamentos_documento_pago": 1,
                    "valor_comissao_total_documento": 500.0,
                }
            ]
        )

        candidates, full_history = apply_paid_history_filter(
            df_machine,
            paid_summary,
            paid_document_summary=paid_document_summary,
        )

        self.assertEqual(candidates["Nro Documento"].tolist(), ["000124"])
        self.assertTrue(full_history.loc[0, "Bloqueado por Pagamento Histórico"])
        self.assertFalse(full_history.loc[1, "Bloqueado por Pagamento Histórico"])
        self.assertIn("documento", full_history.loc[0, "Status Confronto Pagas"].lower())
        self.assertIn("outro documento", full_history.loc[1, "Status Confronto Pagas"].lower())

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

    def test_paid_history_blocks_document_without_chassi(self):
        df_machine = pd.DataFrame(
            [
                {"Nro Chassi": "", "Nro Documento": "000122580", "Valor Comissão Total": 100.0},
                {"Nro Chassi": "", "Nro Documento": "000999999", "Valor Comissão Total": 100.0},
                {"Nro Chassi": "CH1", "Nro Documento": "000122580", "Valor Comissão Total": 100.0},
            ]
        )
        paid_document_summary = pd.DataFrame(
            [
                {
                    "nro_chassi": "",
                    "nro_documento": "122580",
                    "qtd_lancamentos_documento_pago": 1,
                    "valor_comissao_total_documento": 100.0,
                }
            ]
        )

        candidates, full_history = apply_paid_history_filter(
            df_machine,
            paid_summary=None,
            paid_document_summary=paid_document_summary,
        )

        self.assertEqual(full_history["Bloqueado por Pagamento Histórico"].tolist(), [True, False, False])
        self.assertEqual(candidates["Nro Documento"].tolist(), ["000999999", "000122580"])
        self.assertEqual(full_history.loc[0, "Status Confronto Pagas"], "Já tratado neste documento")
        self.assertEqual(full_history.loc[1, "Status Confronto Pagas"], "Novo para apuração")

    def test_excluded_document_summary_blocks_row_without_chassi(self):
        df_machine = pd.DataFrame(
            [
                {"Nro Chassi": "", "Nro Documento": "000122580", "Valor Comissão Total": 100.0},
                {"Nro Chassi": "", "Nro Documento": "000999999", "Valor Comissão Total": 100.0},
            ]
        )
        excluded_document_summary = pd.DataFrame(
            [{"nro_chassi": "", "nro_documento": "122580", "qtd_lancamentos_excluidos": 1}]
        )

        candidates, full_history = apply_paid_history_filter(
            df_machine,
            paid_summary=None,
            excluded_document_summary=excluded_document_summary,
        )

        self.assertEqual(full_history["Bloqueado por Exclusão"].tolist(), [True, False])
        self.assertEqual(full_history.loc[0, "Status Confronto Pagas"], "Excluído manualmente")
        self.assertEqual(candidates["Nro Documento"].tolist(), ["000999999"])

    def test_filter_old_emissions_drops_faturamento_devolucao_before_cutoff(self):
        df = pd.DataFrame(
            [
                {"Tipo": "Faturamento", "Data de Emissão": "30/11/2025", "Nro Chassi": "A"},
                {"Tipo": "Devolução", "Data de Emissão": "15/10/2025", "Nro Chassi": "B"},
                {"Tipo": "Faturamento", "Data de Emissão": "01/12/2025", "Nro Chassi": "C"},
                {"Tipo": "Faturamento", "Data de Emissão": "10/01/2026", "Nro Chassi": "D"},
                {"Tipo": "Outro", "Data de Emissão": "01/01/2024", "Nro Chassi": "E"},
            ]
        )

        filtered, dropped = filter_old_emissions(df, enabled=True)

        self.assertEqual(dropped, 2)
        self.assertEqual(filtered["Nro Chassi"].tolist(), ["C", "D", "E"])

    def test_filter_old_emissions_noop_when_disabled(self):
        df = pd.DataFrame(
            [
                {"Tipo": "Faturamento", "Data de Emissão": "01/01/2024", "Nro Chassi": "A"},
            ]
        )
        filtered, dropped = filter_old_emissions(df, enabled=False)
        self.assertEqual(dropped, 0)
        self.assertEqual(len(filtered), 1)

    def test_venda_direta_uses_six_percent_and_ignores_margin_rules(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Venda Direta",
                    "Modelo": "",
                    "Receita Bruta": 100000.0,
                    "% Margem Bruta": 40.0,
                },
                {
                    "Classificacao Venda": "Venda Direta",
                    "Modelo": "6125J",
                    "Receita Bruta": 250000.0,
                    "% Margem Bruta": 40.0,
                },
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 9.0}])
        margin_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 2.0, "meta_margem": 10.0}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertEqual(result["% Comissão Fat."].tolist(), [6.0, 6.0])
        self.assertEqual(result["Valor Comissão Fat."].tolist(), [6000.0, 15000.0])
        self.assertEqual(result["% Comissão Margem"].tolist(), [0.0, 0.0])
        self.assertEqual(result["Meta de Margem"].tolist(), [0.0, 0.0])
        self.assertEqual(result["Valor Comissão Margem"].tolist(), [0.0, 0.0])
        self.assertEqual(result["Valor Comissão Total"].tolist(), [6000.0, 15000.0])
        self.assertEqual(result["Gatilho Comissão Margem Atingido"].tolist(), [False, False])
        self.assertTrue(result["Regra Classificação Aplicada"].all())

    def test_venda_direta_manager_keeps_thirty_three_percent(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificação Venda": "Venda Direta",
                    "Cod Vendedor": "aaetbu",
                    "Valor Comissão Total": 6000.0,
                }
            ]
        )
        relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "AAETBU",
                    "gerente": "Clayton Monteiro de Castro Filho",
                    "percentual_comissao_gerente": 33.0,
                }
            ]
        )

        result = apply_manager_commission_rules(df_machine, relations)

        self.assertEqual(result.loc[0, "% Comissão Gerente"], 33.0)
        self.assertAlmostEqual(result.loc[0, "Valor Comissão Gerente"], 1980.0, places=2)


def _raw_machine_rows(rows):
    """rows: (classificacao, modelo, chassi, cod_vendedor)"""
    return pd.DataFrame(
        [
            {
                "Tipo": "Faturamento",
                "Filial": "Goiânia",
                "Nro Documento": str(index + 1),
                "Classificação Venda": classificacao,
                "Modelo": modelo,
                "Nro Chassi": chassi,
                "Cod Vendedor": cod_vendedor,
                "CEN": f"CEN {cod_vendedor}" if cod_vendedor else "",
                "Receita Bruta": 100000.0,
            }
            for index, (classificacao, modelo, chassi, cod_vendedor) in enumerate(rows)
        ]
    )


class PendingRegistrationTests(unittest.TestCase):
    def test_pending_lists_models_without_fat_rule_grouped(self):
        raw = _raw_machine_rows(
            [
                ("Maquinas JD - Novos", "6110J", "CH1", "001"),
                ("Maquinas JD - Novos", "6110J", "CH2", "001"),
                ("Maquinas JD - Novos", "  ", "CH3", "001"),
                ("Maquinas JD - Novos", "6125J", "CH5", "001"),
                ("Implemento", "XYZ", "CH4", "001"),
            ]
        )
        df, _ = prepare_machine_apuracao(
            raw,
            fat_rates=pd.DataFrame([{"modelo": "6125J", "percentual": 0.5}]),
            margin_rates=None,
            paid_summary=None,
            manager_relations=pd.DataFrame([{"cod_vendedor": "001", "gerente": "G1"}]),
        )

        models, cens = find_pending_registrations(df)

        self.assertEqual(list(models.columns), PENDING_MODEL_COLUMNS)
        self.assertEqual(models["Modelo"].tolist(), ["(sem modelo)", "6110J"])
        by_model = models.set_index("Modelo")
        self.assertEqual(by_model.loc["6110J", "Qtd Linhas"], 2)
        self.assertEqual(by_model.loc["6110J", "Chassi Exemplo"], "CH1")
        self.assertEqual(by_model.loc["(sem modelo)", "Chassi Exemplo"], "CH3")
        self.assertTrue(cens.empty)

    def test_pending_lists_cen_without_manager_and_blank_manager(self):
        raw = _raw_machine_rows(
            [
                ("Implemento", "XYZ", "CH1", "001"),
                ("Implemento", "XYZ", "CH2", "002"),
                ("Implemento", "XYZ", "CH3", "003"),
                ("Implemento", "XYZ", "CH4", "003"),
                ("Implemento", "XYZ", "CH5", ""),
            ]
        )
        df, _ = prepare_machine_apuracao(
            raw,
            fat_rates=None,
            margin_rates=None,
            paid_summary=None,
            manager_relations=pd.DataFrame(
                [{"cod_vendedor": "001", "gerente": "G1"}, {"cod_vendedor": "002", "gerente": ""}]
            ),
        )

        models, cens = find_pending_registrations(df)

        self.assertTrue(models.empty)
        self.assertEqual(list(cens.columns), PENDING_CEN_COLUMNS)
        self.assertEqual(sorted(cens["Cod Vendedor"].tolist()), ["(sem código)", "002", "003"])
        self.assertEqual(cens.set_index("Cod Vendedor").loc["003", "Qtd Linhas"], 2)

    def test_pending_on_empty_dataframe(self):
        models, cens = find_pending_registrations(pd.DataFrame())

        self.assertTrue(models.empty)
        self.assertTrue(cens.empty)
        self.assertEqual(list(models.columns), PENDING_MODEL_COLUMNS)
        self.assertEqual(list(cens.columns), PENDING_CEN_COLUMNS)


if __name__ == "__main__":
    unittest.main()
