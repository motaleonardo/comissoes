import unittest

import pandas as pd

from commission_tool.core.reports import (
    build_cen_report,
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
        self.assertEqual(marcio_row["Comissão Invasão de área"], 0.0)
        self.assertEqual(marcio_row["Comissão Margem"], 20.0)
        self.assertEqual(marcio_row["Valor Comissão Total"], 150.0)

        carla_row = report_df.loc[report_df["CEN"] == "CARLA SOUZA"].iloc[0]
        self.assertEqual(carla_row["Comissão Invasão de área"], 40.0)
        self.assertEqual(carla_row["Comissão Margem"], 5.0)
        self.assertEqual(carla_row["Valor Comissão Total"], 45.0)

        self.assertEqual(len(pending_df), 2)
        self.assertEqual(
            pending_df["Pendência"].tolist(),
            ["Sem cod_vendedor", "Cod_vendedor sem relação CEN x Gerente"],
        )
        self.assertEqual(pending_df["Valor Comissão Total"].tolist(), [50.0, 60.0])

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
                    "perc_margem_bruta": 12.0,
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
        self.assertEqual(implemento_row["Valor Comissão Margem"], 300.0)
        self.assertEqual(implemento_row["Valor Total da Comissão"], 600.0)

        usados_row = report_df.loc[report_df["Tipo"] == "Maquinas JD - Usados"].iloc[0]
        self.assertEqual(usados_row["Receita Bruta"], 80000.0)
        self.assertEqual(usados_row["Valor Comissão Fat."], 160.0)
        self.assertEqual(usados_row["Margem + Incentivos R$"], 5000.0)
        self.assertEqual(usados_row["Meta Margem"], 15.0)
        self.assertEqual(usados_row["% MB Realizado"], 12.0)
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


if __name__ == "__main__":
    unittest.main()
