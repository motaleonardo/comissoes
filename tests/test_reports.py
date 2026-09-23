import unittest

import pandas as pd

from commission_tool.core.reports import (
    build_cen_analytic_reports,
    build_cen_report,
    build_consorcio_coordinator_report,
    build_filial_analytic_reports,
    build_manager_report,
    build_used_implements_analytic_report,
    build_used_implements_coordinator_report,
)


class ReportsTests(unittest.TestCase):
    def test_build_cen_report_groups_values_by_cen_and_keeps_pending_section(self):
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "MARCIO ALVES",
                    "gerente": "hernane borges da costa",
                },
                {
                    "cod_vendedor": "A002",
                    "vendedor": "LUCAS MENDES",
                    "gerente": "lucas mendes",
                },
                {
                    "cod_vendedor": "A003",
                    "vendedor": "CARLA SOUZA",
                    "gerente": "joao gerente",
                },
            ]
        )
        paid_commissions = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A001",
                    "cen": "MARCIO ALVES",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "valor_comissao_fat": 100.0,
                    "valor_comissao_margem": 20.0,
                    "valor_comissao_total": 120.0,
                    "nro_documento": "1",
                    "nro_chassi": "CH1",
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A001",
                    "cen": "MARCIO ALVES",
                    "classificacao_venda": "Venda Direta",
                    "valor_comissao_fat": 30.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 30.0,
                    "nro_documento": "2",
                    "nro_chassi": "CH2",
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A003",
                    "cen": "CARLA SOUZA",
                    "classificacao_venda": "Invasão de área",
                    "valor_comissao_fat": 40.0,
                    "valor_comissao_margem": 5.0,
                    "valor_comissao_total": 45.0,
                    "nro_documento": "3",
                    "nro_chassi": "CH3",
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "",
                    "cen": "SEM CODIGO",
                    "classificacao_venda": "Implemento",
                    "valor_comissao_fat": 50.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 50.0,
                    "nro_documento": "4",
                    "nro_chassi": "CH4",
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A999",
                    "cen": "NAO RELACIONADO",
                    "classificacao_venda": "Maquinas JD - Usados",
                    "valor_comissao_fat": 60.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 60.0,
                    "nro_documento": "5",
                    "nro_chassi": "CH5",
                },
            ]
        )

        report_df, pending_df = build_cen_report(
            paid_commissions=paid_commissions,
            manager_relations=manager_relations,
            period_label="Abril/2026",
        )

        self.assertEqual(report_df["CEN"].tolist(), ["CARLA SOUZA", "MARCIO ALVES"])
        self.assertNotIn("LUCAS MENDES", report_df["CEN"].tolist())

        marcio_row = report_df.loc[report_df["CEN"] == "MARCIO ALVES"].iloc[0]
        self.assertEqual(marcio_row["Comissão Faturamento"], 100.0)
        self.assertEqual(marcio_row["Venda Direta"], 30.0)
        self.assertEqual(marcio_row["Comissão Usados"], 0.0)
        self.assertEqual(marcio_row["Comissão Implementos"], 0.0)
        self.assertEqual(marcio_row["Outros"], 0.0)
        self.assertEqual(marcio_row["Comissão Margem"], 20.0)
        self.assertEqual(marcio_row["Valor Comissão Total"], 150.0)

        carla_row = report_df.loc[report_df["CEN"] == "CARLA SOUZA"].iloc[0]
        self.assertEqual(carla_row["Outros"], 40.0)
        self.assertEqual(carla_row["Comissão Margem"], 5.0)
        self.assertEqual(carla_row["Valor Comissão Total"], 45.0)

        self.assertEqual(len(pending_df), 2)
        self.assertEqual(
            pending_df["Pendência"].tolist(),
            ["Sem cod_vendedor", "Cod_vendedor sem relação CEN x Gerente"],
        )
        self.assertEqual(pending_df["Valor Comissão Total"].tolist(), [50.0, 60.0])

    def test_build_cen_report_outros_column_aggregates_invasao_de_area_and_outro(self):
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "MARCIO ALVES",
                    "gerente": "hernane",
                },
            ]
        )
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A001",
                    "cen": "MARCIO ALVES",
                    "classificacao_venda": "Invasão de área",
                    "valor_comissao_fat": 10.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 10.0,
                    "nro_documento": "1",
                    "nro_chassi": "CH1",
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A001",
                    "cen": "MARCIO ALVES",
                    "classificacao_venda": "Outro",
                    "valor_comissao_fat": 25.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 25.0,
                    "nro_documento": "2",
                    "nro_chassi": "CH2",
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A001",
                    "cen": "MARCIO ALVES",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "valor_comissao_fat": 100.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 100.0,
                    "nro_documento": "3",
                    "nro_chassi": "CH3",
                },
            ]
        )

        report_df, _ = build_cen_report(paid, manager_relations, "Abril/2026")

        row = report_df.iloc[0]
        self.assertEqual(row["Comissão Faturamento"], 100.0)
        # Outros agrega Invasão de área (10) + Outro (25) = 35
        self.assertEqual(row["Outros"], 35.0)
        # Total = 100 + 35 = 135
        self.assertEqual(row["Valor Comissão Total"], 135.0)
        # Coluna antiga não deve existir mais
        self.assertNotIn("Comissão Invasão de área", report_df.columns)

    def test_build_cen_report_keeps_all_valid_sellers_even_without_commissions(self):
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "MARCIO ALVES",
                    "gerente": "hernane",
                },
                {
                    "cod_vendedor": "A002",
                    "vendedor": "CARLA SOUZA",
                    "gerente": "joao",
                },
            ]
        )

        report_df, pending_df = build_cen_report(
            paid_commissions=pd.DataFrame(columns=["mes_ano_comissao", "cod_vendedor"]),
            manager_relations=manager_relations,
            period_label="Abril/2026",
        )

        self.assertEqual(report_df["CEN"].tolist(), ["CARLA SOUZA", "MARCIO ALVES"])
        self.assertTrue((report_df["Valor Comissão Total"] == 0).all())
        self.assertTrue(pending_df.empty)

    def test_build_manager_report_aggregates_team_sales_and_manager_own_commission(self):
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "ALICE VENDAS",
                    "gerente": "joao gerente",
                },
                {
                    "cod_vendedor": "A002",
                    "vendedor": "BOB VENDAS",
                    "gerente": "joao gerente",
                },
                {
                    "cod_vendedor": "A003",
                    "vendedor": "JOAO GERENTE",
                    "gerente": "joao gerente",
                },
                {
                    "cod_vendedor": "B001",
                    "vendedor": "MARIA GESTORA",
                    "gerente": "maria gestora",
                },
            ]
        )
        paid_commissions = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A001",
                    "gerente": "joao gerente",
                    "cen": "ALICE VENDAS",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 1000.0,
                    "margem_incentivos_rs": 200.0,
                    "receita_bruta_incentivos_rs": 1200.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_fat": 100.0,
                    "valor_comissao_margem": 20.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A002",
                    "gerente": "joao gerente",
                    "cen": "BOB VENDAS",
                    "classificacao_venda": "Implemento",
                    "receita_bruta": 500.0,
                    "margem_incentivos_rs": 80.0,
                    "receita_bruta_incentivos_rs": 600.0,
                    "perc_margem_bruta": 20.0,
                    "valor_comissao_fat": 50.0,
                    "valor_comissao_margem": 0.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A003",
                    "gerente": "joao gerente",
                    "cen": "JOAO GERENTE",
                    "classificacao_venda": "Venda Direta",
                    "receita_bruta": 400.0,
                    "margem_incentivos_rs": 100.0,
                    "receita_bruta_incentivos_rs": 500.0,
                    "perc_margem_bruta": 30.0,
                    "valor_comissao_fat": 40.0,
                    "valor_comissao_margem": 5.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "B001",
                    "gerente": "maria gestora",
                    "cen": "MARIA GESTORA",
                    "classificacao_venda": "Maquinas JD - Usados",
                    "receita_bruta": 300.0,
                    "margem_incentivos_rs": 50.0,
                    "receita_bruta_incentivos_rs": 350.0,
                    "perc_margem_bruta": 40.0,
                    "valor_comissao_fat": 30.0,
                    "valor_comissao_margem": 0.0,
                },
            ]
        )

        report_df = build_manager_report(
            paid_commissions=paid_commissions,
            manager_relations=manager_relations,
            period_label="Abril/2026",
        )

        self.assertEqual(report_df["Gerente"].tolist(), ["joao gerente", "maria gestora"])

        joao_row = report_df.loc[report_df["Gerente"] == "joao gerente"].iloc[0]
        self.assertEqual(joao_row["Receita Bruta"], 1900.0)
        self.assertEqual(joao_row["Margem + Incentivos R$"], 380.0)
        self.assertAlmostEqual(joao_row["% Margem Bruta"], (10.0 * 1200.0 + 20.0 * 600.0 + 30.0 * 500.0) / 2300.0)
        self.assertEqual(joao_row["Comissão Total CEN"], 215.0)
        self.assertAlmostEqual(joao_row["Comissão Gerente"], 70.95)

        maria_row = report_df.loc[report_df["Gerente"] == "maria gestora"].iloc[0]
        self.assertEqual(maria_row["Receita Bruta"], 300.0)
        self.assertEqual(maria_row["Margem + Incentivos R$"], 50.0)
        self.assertEqual(maria_row["% Margem Bruta"], 40.0)
        self.assertEqual(maria_row["Comissão Total CEN"], 30.0)
        self.assertAlmostEqual(maria_row["Comissão Gerente"], 9.9)

    def test_build_manager_report_uses_team_commission_even_when_manager_is_not_a_seller(self):
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "ALICE VENDAS",
                    "gerente": "joao gerente",
                }
            ]
        )
        paid_commissions = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "cod_vendedor": "A001",
                    "gerente": "joao gerente",
                    "cen": "ALICE VENDAS",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 1000.0,
                    "margem_incentivos_rs": 200.0,
                    "receita_bruta_incentivos_rs": 1000.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_fat": 100.0,
                    "valor_comissao_margem": 20.0,
                }
            ]
        )

        report_df = build_manager_report(
            paid_commissions=paid_commissions,
            manager_relations=manager_relations,
            period_label="Abril/2026",
        )

        joao_row = report_df.loc[report_df["Gerente"] == "joao gerente"].iloc[0]
        self.assertEqual(joao_row["Receita Bruta"], 1000.0)
        self.assertEqual(joao_row["Comissão Total CEN"], 120.0)
        self.assertAlmostEqual(joao_row["Comissão Gerente"], 39.6)

    def test_build_used_implements_coordinator_report(self):
        paid_commissions = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "classificacao_venda": "Implemento",
                    "receita_bruta": 100000.0,
                    "margem_incentivos_rs": 20000.0,
                    "receita_bruta_incentivos_rs": 120000.0,
                    "perc_margem_bruta": 10.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "classificacao_venda": "Implemento",
                    "receita_bruta": 50000.0,
                    "margem_incentivos_rs": 10000.0,
                    "receita_bruta_incentivos_rs": 55000.0,
                    "perc_margem_bruta": 20.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "classificacao_venda": "Maquinas JD - Usados",
                    "receita_bruta": 80000.0,
                    "margem_incentivos_rs": 5000.0,
                    "receita_bruta_incentivos_rs": 85000.0,
                    "perc_margem_bruta": 16.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 99999.0,
                    "margem_incentivos_rs": 9999.0,
                    "receita_bruta_incentivos_rs": 100000.0,
                    "perc_margem_bruta": 50.0,
                },
            ]
        )

        report_df = build_used_implements_coordinator_report(
            paid_commissions=paid_commissions,
            period_label="Abril/2026",
        )

        self.assertEqual(report_df["Nome"].tolist(), ["Wagner Goncalves Garcia", "Wagner Goncalves Garcia"])
        self.assertEqual(report_df["Tipo"].tolist(), ["Implemento", "Maquinas JD - Usados"])

        implemento_row = report_df.loc[report_df["Tipo"] == "Implemento"].iloc[0]
        self.assertEqual(implemento_row["Receita Bruta"], 150000.0)
        self.assertEqual(implemento_row["% Comissão Fat."], 0.2)
        self.assertEqual(implemento_row["Valor Comissão Fat."], 300.0)
        self.assertEqual(implemento_row["Margem + Incentivos R$"], 30000.0)
        self.assertEqual(implemento_row["Meta Margem"], 15.0)
        self.assertAlmostEqual(implemento_row["% MB Realizado"], (10.0 * 120000.0 + 20.0 * 55000.0) / 175000.0)
        self.assertEqual(implemento_row["% Comissão Margem"], 0.2)
        self.assertEqual(implemento_row["Valor Comissão Margem"], 0.0)
        self.assertEqual(implemento_row["Valor Total da Comissão"], 300.0)

        usados_row = report_df.loc[report_df["Tipo"] == "Maquinas JD - Usados"].iloc[0]
        self.assertEqual(usados_row["Receita Bruta"], 80000.0)
        self.assertEqual(usados_row["Valor Comissão Fat."], 160.0)
        self.assertEqual(usados_row["Margem + Incentivos R$"], 5000.0)
        self.assertEqual(usados_row["Meta Margem"], 15.0)
        self.assertEqual(usados_row["% MB Realizado"], 16.0)
        self.assertEqual(usados_row["Valor Comissão Margem"], 160.0)
        self.assertEqual(usados_row["Valor Total da Comissão"], 320.0)

    def test_build_filial_analytic_reports_groups_only_nonzero_commission_filiais(self):
        paid_commissions = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja A",
                    "nro_documento": "1",
                    "nro_chassi": "CH1",
                    "nome_cliente": "Cliente 1",
                    "cen": "CEN 1",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 1000.0,
                    "valor_comissao_fat": 100.0,
                    "margem_rs": 200.0,
                    "perc_margem_direta": 20.0,
                    "valor_incentivo": 10.0,
                    "margem_incentivos_rs": 210.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 21.0,
                    "valor_comissao_margem": 20.0,
                    "valor_comissao_total": 120.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja B",
                    "nro_documento": "2",
                    "nro_chassi": "CH2",
                    "nome_cliente": "Cliente 2",
                    "cen": "CEN 2",
                    "classificacao_venda": "Implemento",
                    "receita_bruta": 500.0,
                    "valor_comissao_fat": 0.0,
                    "margem_rs": 50.0,
                    "perc_margem_direta": 10.0,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 50.0,
                    "meta_margem": 0.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 0.0,
                },
            ]
        )

        reports = build_filial_analytic_reports(paid_commissions, "Abril/2026")

        self.assertEqual(len(reports), 1)
        filial, filial_df = reports[0]
        self.assertEqual(filial, "Loja A")
        self.assertEqual(filial_df.loc[0, "Filial"], "Loja A")
        self.assertEqual(filial_df.loc[0, "Valor Comissão Total"], 120.0)

    def test_build_used_implements_analytic_report_filters_only_requested_types(self):
        paid_commissions = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja A",
                    "nro_documento": "1",
                    "nro_chassi": "CH1",
                    "nome_cliente": "Cliente 1",
                    "cen": "CEN 1",
                    "classificacao_venda": "Implemento",
                    "receita_bruta": 1000.0,
                    "valor_comissao_fat": 100.0,
                    "margem_rs": 200.0,
                    "perc_margem_direta": 20.0,
                    "valor_incentivo": 10.0,
                    "margem_incentivos_rs": 210.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 21.0,
                    "valor_comissao_margem": 20.0,
                    "valor_comissao_total": 120.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja A",
                    "nro_documento": "2",
                    "nro_chassi": "CH2",
                    "nome_cliente": "Cliente 2",
                    "cen": "CEN 2",
                    "classificacao_venda": "Maquinas JD - Usados",
                    "receita_bruta": 800.0,
                    "valor_comissao_fat": 80.0,
                    "margem_rs": 100.0,
                    "perc_margem_direta": 12.5,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 100.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 12.5,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 80.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja A",
                    "nro_documento": "3",
                    "nro_chassi": "CH3",
                    "nome_cliente": "Cliente 3",
                    "cen": "CEN 3",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 5000.0,
                    "valor_comissao_fat": 500.0,
                    "margem_rs": 1000.0,
                    "perc_margem_direta": 20.0,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 1000.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 20.0,
                    "valor_comissao_margem": 50.0,
                    "valor_comissao_total": 550.0,
                },
            ]
        )

        report_df = build_used_implements_analytic_report(paid_commissions, "Abril/2026")

        self.assertEqual(report_df["Classificação Venda"].tolist(), ["Implemento", "Maquinas JD - Usados"])
        self.assertEqual(report_df["Nro Documento"].tolist(), ["1", "2"])
        self.assertIn("Receita Bruta", report_df.columns)
        self.assertNotIn("Valor Comissão Fat.", report_df.columns)
        self.assertNotIn("Valor Comissão Margem", report_df.columns)
        self.assertNotIn("Valor Comissão Total", report_df.columns)

    def test_build_cen_analytic_reports_skips_cens_with_zero_total_and_sorts_by_name(self):
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja A",
                    "nro_documento": "1",
                    "nro_chassi": "CH1",
                    "nome_cliente": "Cliente 1",
                    "cen": "MARCIO ALVES",
                    "cod_vendedor": "A001",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 1000.0,
                    "valor_comissao_fat": 50.0,
                    "margem_rs": 100.0,
                    "perc_margem_direta": 10.0,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 100.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 50.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja A",
                    "nro_documento": "2",
                    "nro_chassi": "CH2",
                    "nome_cliente": "Cliente 2",
                    "cen": "MARCIO ALVES",
                    "cod_vendedor": "A001",
                    "classificacao_venda": "Implemento",
                    "receita_bruta": 500.0,
                    "valor_comissao_fat": 5.0,
                    "margem_rs": 50.0,
                    "perc_margem_direta": 10.0,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 50.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 5.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja B",
                    "nro_documento": "3",
                    "nro_chassi": "CH3",
                    "nome_cliente": "Cliente 3",
                    "cen": "CARLA SOUZA",
                    "cod_vendedor": "A002",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 2000.0,
                    "valor_comissao_fat": 200.0,
                    "margem_rs": 400.0,
                    "perc_margem_direta": 20.0,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 400.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 20.0,
                    "valor_comissao_margem": 100.0,
                    "valor_comissao_total": 300.0,
                },
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja C",
                    "nro_documento": "4",
                    "nro_chassi": "CH4",
                    "nome_cliente": "Cliente 4",
                    "cen": "LUCAS MENDES",
                    "cod_vendedor": "A003",
                    "classificacao_venda": "Implemento",
                    "receita_bruta": 800.0,
                    "valor_comissao_fat": 0.0,
                    "margem_rs": 0.0,
                    "perc_margem_direta": 0.0,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 0.0,
                    "meta_margem": 0.0,
                    "perc_margem_bruta": 0.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 0.0,
                },
            ]
        )

        reports = build_cen_analytic_reports(paid, "Abril/2026")

        # Apenas dois CENs com valor > 0 — LUCAS MENDES (total = 0) é excluído
        self.assertEqual(len(reports), 2)
        names = [name for name, _, _ in reports]
        self.assertEqual(names, ["CARLA SOUZA", "MARCIO ALVES"])

        carla_name, carla_cod, carla_df = reports[0]
        self.assertEqual(carla_cod, "A002")
        self.assertEqual(carla_df["Nro Documento"].tolist(), ["3"])

        marcio_name, marcio_cod, marcio_df = reports[1]
        self.assertEqual(marcio_cod, "A001")
        self.assertEqual(marcio_df["Nro Documento"].tolist(), ["1", "2"])
        self.assertAlmostEqual(
            marcio_df["Valor Comissão Total"].sum(), 55.0,
        )

    def test_analytic_report_includes_product_code(self):
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Setembro/2026",
                    "cod_vendedor": "AAETBU",
                    "cen": "ABNER SAMUEL FERREIRA COSTA",
                    "filial": "Loja Vianopolis",
                    "classificacao_venda": "Venda Direta",
                    "produto_codigo": "CVD JD",
                    "nro_documento": "000122580",
                    "valor_comissao_fat": 6000.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 6000.0,
                }
            ]
        )

        reports = build_cen_analytic_reports(paid, "Setembro/2026")

        self.assertEqual(len(reports), 1)
        _, _, analytic = reports[0]
        self.assertIn("Produto Código", analytic.columns)
        self.assertEqual(analytic.loc[0, "Produto Código"], "CVD JD")

    def test_build_cen_analytic_reports_returns_empty_when_no_cen_has_value(self):
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "filial": "Loja A",
                    "nro_documento": "1",
                    "cen": "X",
                    "cod_vendedor": "A001",
                    "valor_comissao_total": 0.0,
                },
            ]
        )
        self.assertEqual(build_cen_analytic_reports(paid, "Abril/2026"), [])

    def test_analytic_reports_include_invoice_issue_date(self):
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Abril/2026",
                    "tipo": "Faturamento",
                    "filial": "Loja A",
                    "data_emissao": "2026-04-10",
                    "nro_documento": "1",
                    "nro_chassi": "CH1",
                    "nome_cliente": "Cliente 1",
                    "cen": "MARCIO ALVES",
                    "cod_vendedor": "A001",
                    "classificacao_venda": "Maquinas JD - Novos",
                    "receita_bruta": 1000.0,
                    "valor_comissao_fat": 50.0,
                    "margem_rs": 100.0,
                    "perc_margem_direta": 10.0,
                    "valor_incentivo": 0.0,
                    "margem_incentivos_rs": 100.0,
                    "meta_margem": 15.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 50.0,
                }
            ]
        )

        reports = build_cen_analytic_reports(paid, "Abril/2026")
        _, _, analytic_df = reports[0]

        self.assertIn("Tipo", analytic_df.columns)
        self.assertEqual(analytic_df.loc[0, "Tipo"], "Faturamento")
        self.assertIn("Data de Emissão", analytic_df.columns)
        self.assertEqual(analytic_df.loc[0, "Data de Emissão"], "10/04/2026")


CONSORCIO_PAID = pd.DataFrame(
    [
        {
            "mes_ano_comissao": "Setembro/2026",
            "cod_vendedor": "00188",
            "cen": "VENDEDOR A",
            "filial": "Loja Acreuna",
            "gerente": "Reginaldo Pereira Garcia",
            "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
            "vl_bem": 200000.0,
            "valor_comissao_cen": 1680.0,
            "valor_comissao_gestor": 0.0,
            "valor_comissao_coordenador": 0.0,
        },
        {
            "mes_ano_comissao": "Setembro/2026",
            "cod_vendedor": "00188",
            "cen": "VENDEDOR A",
            "filial": "Loja Acreuna",
            "gerente": "Reginaldo Pereira Garcia",
            "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
            "vl_bem": 206994.15,
            "valor_comissao_cen": 0.0,
            "valor_comissao_gestor": 434.687715,
            "valor_comissao_coordenador": 310.491225,
        },
        {
            "mes_ano_comissao": "Agosto/2026",
            "cod_vendedor": "00188",
            "cen": "VENDEDOR A",
            "filial": "Loja Acreuna",
            "gerente": "Reginaldo Pereira Garcia",
            "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
            "vl_bem": 100000.0,
            "valor_comissao_cen": 840.0,
            "valor_comissao_gestor": 0.0,
            "valor_comissao_coordenador": 0.0,
        },
    ]
)


class ConsorcioPendingInCenReportTests(unittest.TestCase):
    def test_consorcio_cod_vendedor_without_relation_becomes_pendencia_not_silently_dropped(self):
        """Regression for C2 item 3: a consortium cota paid to a cod_vendedor that has
        no active CEN x Gerente relation (e.g. the relation was deactivated after the
        cota was recorded as paid) must show up as a pendência, never disappear."""
        relations = pd.DataFrame(
            [{"cod_vendedor": "00188", "vendedor": "VENDEDOR A", "gerente": "Reginaldo Pereira Garcia"}]
        )
        consorcio_orfao = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Setembro/2026",
                    "cod_vendedor": "SEMRELACAO",
                    "cen": "VENDEDOR SEM RELACAO",
                    "filial": "Loja Acreuna",
                    "gerente": "",
                    "coordenador": "",
                    "vl_bem": 100000.0,
                    "valor_comissao_cen": 840.0,
                    "valor_comissao_gestor": 0.0,
                    "valor_comissao_coordenador": 0.0,
                }
            ]
        )

        report, pending = build_cen_report(
            pd.DataFrame(),
            relations,
            "Setembro/2026",
            consorcio_paid=consorcio_orfao,
        )

        self.assertNotIn("VENDEDOR SEM RELACAO", report["CEN"].tolist())
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending.loc[0, "Cod Vendedor"], "SEMRELACAO")
        self.assertEqual(pending.loc[0, "CEN"], "VENDEDOR SEM RELACAO")
        self.assertEqual(pending.loc[0, "Pendência"], "Cod_vendedor de consórcio sem relação CEN x Gerente")
        self.assertAlmostEqual(pending.loc[0, "Valor Comissão Total"], 840.0, places=2)

    def test_consorcio_pendencia_is_merged_with_machine_pendencia(self):
        relations = pd.DataFrame(
            [{"cod_vendedor": "00188", "vendedor": "VENDEDOR A", "gerente": "Reginaldo Pereira Garcia"}]
        )
        paid_commissions = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Setembro/2026",
                    "cod_vendedor": "A999",
                    "cen": "NAO RELACIONADO",
                    "classificacao_venda": "Maquinas JD - Usados",
                    "valor_comissao_fat": 60.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 60.0,
                    "nro_documento": "5",
                    "nro_chassi": "CH5",
                }
            ]
        )
        consorcio_orfao = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Setembro/2026",
                    "cod_vendedor": "SEMRELACAO",
                    "cen": "VENDEDOR SEM RELACAO",
                    "vl_bem": 100000.0,
                    "valor_comissao_cen": 840.0,
                    "valor_comissao_gestor": 0.0,
                    "valor_comissao_coordenador": 0.0,
                }
            ]
        )

        _, pending = build_cen_report(
            paid_commissions,
            relations,
            "Setembro/2026",
            consorcio_paid=consorcio_orfao,
        )

        self.assertEqual(len(pending), 2)
        self.assertEqual(
            set(pending["Pendência"].tolist()),
            {"Cod_vendedor sem relação CEN x Gerente", "Cod_vendedor de consórcio sem relação CEN x Gerente"},
        )


class ConsorcioInReportsTests(unittest.TestCase):
    def test_cen_report_has_consorcio_column_for_the_period(self):
        relations = pd.DataFrame(
            [{"cod_vendedor": "00188", "vendedor": "VENDEDOR A", "gerente": "Reginaldo Pereira Garcia"}]
        )

        report, _ = build_cen_report(
            pd.DataFrame(),
            relations,
            "Setembro/2026",
            consorcio_paid=CONSORCIO_PAID,
        )

        row = report.loc[report["CEN"] == "VENDEDOR A"].iloc[0]
        self.assertAlmostEqual(row["Consórcio"], 1680.0, places=2)
        self.assertAlmostEqual(row["Valor Comissão Total"], 1680.0, places=2)

    def test_manager_report_adds_consorcio_outside_the_33_percent(self):
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Setembro/2026",
                    "gerente": "Reginaldo Pereira Garcia",
                    "cod_vendedor": "00188",
                    "cen": "VENDEDOR A",
                    "filial": "Loja Acreuna",
                    "classificacao_venda": "Venda Direta",
                    "receita_bruta": 100000.0,
                    "receita_bruta_incentivos_rs": 100000.0,
                    "margem_incentivos_rs": 10000.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_fat": 6000.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 6000.0,
                }
            ]
        )

        report = build_manager_report(paid, None, "Setembro/2026", consorcio_paid=CONSORCIO_PAID)

        row = report.iloc[0]
        self.assertAlmostEqual(row["Gestor Consórcio"], 434.687715, places=6)
        self.assertAlmostEqual(row["Comissão Gerente"], 6000.0 * 0.33 + 434.687715, places=6)

    def test_coordinator_report_sums_only_the_period(self):
        report = build_consorcio_coordinator_report(CONSORCIO_PAID, "Setembro/2026")

        self.assertEqual(report.loc[0, "Nome"], "DJALMA RODRIGUES DE PAULA JUNIOR")
        self.assertAlmostEqual(report.loc[0, "Valor Comissão"], 310.491225, places=6)

    def test_coordinator_report_merges_names_with_different_accent_and_case(self):
        consorcio_paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "Setembro/2026",
                    "cod_vendedor": "00188",
                    "coordenador": "Djalma Rodrigues de Paula Júnior",
                    "vl_bem": 100000.0,
                    "valor_comissao_coordenador": 150.0,
                },
                {
                    "mes_ano_comissao": "Setembro/2026",
                    "cod_vendedor": "00199",
                    "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
                    "vl_bem": 50000.0,
                    "valor_comissao_coordenador": 75.0,
                },
            ]
        )

        report = build_consorcio_coordinator_report(consorcio_paid, "Setembro/2026")

        self.assertEqual(len(report), 1)
        self.assertEqual(report.loc[0, "Nome"], "Djalma Rodrigues de Paula Júnior")
        self.assertAlmostEqual(report.loc[0, "Receita (VL Bem)"], 150000.0, places=2)
        self.assertAlmostEqual(report.loc[0, "Valor Comissão"], 225.0, places=2)


if __name__ == "__main__":
    unittest.main()
