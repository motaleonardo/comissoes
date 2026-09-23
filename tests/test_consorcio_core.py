import unittest

import pandas as pd

from commission_tool.core.consorcio import (
    CONSORCIO_ANALYTIC_COLUMN_MAP,
    STATUS_APTO,
    STATUS_BLOQUEADO_DUPLICADO,
    STATUS_BLOQUEADO_GERENTE,
    STATUS_BLOQUEADO_JA_PAGO,
    STATUS_BLOQUEADO_PDV,
    STATUS_BLOQUEADO_REGRA,
    STATUS_BLOQUEADO_VALOR,
    STATUS_BLOQUEADO_VENDEDOR,
    STATUS_SEM_COMISSAO,
    build_consorcio_analytic_df,
    build_premiacao_tables,
    enrich_consorcio_rows,
    suggest_cod_vendedor,
)


PDV_MAP = pd.DataFrame([{"cd_pdv": "208016", "nm_pdv": "HOHL ACREUNA", "filial": "Loja Acreuna"}])
VENDEDOR_MAP = pd.DataFrame(
    [
        {"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A", "cod_vendedor": "00188"},
        {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B", "cod_vendedor": ""},
        {"cd_vendedor_consorcio": "002356", "nm_vendedor_consorcio": "VENDEDOR C", "cod_vendedor": "A69853"},
    ]
)
RELATIONS = pd.DataFrame(
    [{"cod_vendedor": "00188", "vendedor": "VENDEDOR A", "gerente": "Reginaldo Pereira Garcia"}]
)
REGRAS = pd.DataFrame(
    [
        {"parcela": "001 / 003", "percentual_cen": 0.84, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
        {"parcela": "002 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.21, "percentual_coordenador": 0.15},
        {"parcela": "003 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
    ]
)
COORDENADOR = {"nome": "DJALMA RODRIGUES DE PAULA JUNIOR", "email": "djalma.junior@hohl.com.br"}


def _row(**overrides) -> dict:
    row = {
        "arquivo_origem": "Acreuna.csv",
        "cd_pedido": "4504255710",
        "cd_pdv": "208016",
        "nm_pdv": "HOHL ACREUNA",
        "cd_vendedor_consorcio": "002197",
        "nm_vendedor_consorcio": "VENDEDOR A",
        "consorciado": "CONSORCIADO TESTE",
        "cd_grupo": "002362",
        "cd_cota": "0025",
        "versao": "00",
        "st_contemplacao": "Não Contemplada",
        "parcela": "001 / 003",
        "vl_bem": 200000.0,
    }
    row.update(overrides)
    return row


def _enrich(rows, paid_keys=None):
    return enrich_consorcio_rows(
        pd.DataFrame(rows),
        pdv_map=PDV_MAP,
        vendedor_map=VENDEDOR_MAP,
        manager_relations=RELATIONS,
        regras=REGRAS,
        coordenador=COORDENADOR,
        paid_keys=paid_keys,
    )


class EnrichTests(unittest.TestCase):
    def test_first_installment_pays_cen_only(self):
        result = _enrich([_row()])

        self.assertEqual(result.loc[0, "filial"], "Loja Acreuna")
        self.assertEqual(result.loc[0, "cod_vendedor"], "00188")
        self.assertEqual(result.loc[0, "cen"], "VENDEDOR A")
        self.assertEqual(result.loc[0, "gerente"], "Reginaldo Pereira Garcia")
        self.assertAlmostEqual(result.loc[0, "valor_comissao_cen"], 1680.0, places=2)
        self.assertEqual(result.loc[0, "valor_comissao_gestor"], 0.0)
        self.assertEqual(result.loc[0, "valor_comissao_coordenador"], 0.0)
        self.assertEqual(result.loc[0, "status"], STATUS_APTO)
        self.assertTrue(result.loc[0, "apto"])

    def test_second_installment_pays_manager_and_coordinator(self):
        result = _enrich([_row(parcela="002 / 003", vl_bem=206994.15)])

        self.assertEqual(result.loc[0, "valor_comissao_cen"], 0.0)
        self.assertAlmostEqual(result.loc[0, "valor_comissao_gestor"], 434.687715, places=6)
        self.assertAlmostEqual(result.loc[0, "valor_comissao_coordenador"], 310.491225, places=6)
        self.assertEqual(result.loc[0, "coordenador"], "DJALMA RODRIGUES DE PAULA JUNIOR")
        self.assertEqual(result.loc[0, "status"], STATUS_APTO)

    def test_third_installment_has_no_commission(self):
        result = _enrich([_row(parcela="003 / 003")])

        self.assertEqual(result.loc[0, "status"], STATUS_SEM_COMISSAO)
        self.assertFalse(result.loc[0, "apto"])
        self.assertEqual(result.loc[0, "valor_comissao_cen"], 0.0)

    def test_blocks_unknown_pdv(self):
        result = _enrich([_row(cd_pdv="002001")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_PDV)
        self.assertFalse(result.loc[0, "apto"])

    def test_pdv_matches_ignoring_leading_zeros(self):
        result = _enrich([_row(cd_pdv="0208016")])

        self.assertEqual(result.loc[0, "filial"], "Loja Acreuna")

    def test_blocks_seller_without_de_para(self):
        result = _enrich([_row(cd_vendedor_consorcio="002697", nm_vendedor_consorcio="VENDEDOR B")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_VENDEDOR)

    def test_blocks_unknown_seller_not_in_map(self):
        result = _enrich([_row(cd_vendedor_consorcio="009999", nm_vendedor_consorcio="VENDEDOR NOVO")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_VENDEDOR)

    def test_blocks_second_installment_without_manager(self):
        result = _enrich(
            [_row(cd_vendedor_consorcio="002356", nm_vendedor_consorcio="VENDEDOR C", parcela="002 / 003")]
        )

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_GERENTE)

    def test_first_installment_does_not_require_manager(self):
        result = _enrich([_row(cd_vendedor_consorcio="002356", nm_vendedor_consorcio="VENDEDOR C")])

        self.assertEqual(result.loc[0, "status"], STATUS_APTO)

    def test_blocks_already_paid_cota(self):
        result = _enrich([_row()], paid_keys={"002362||0025||00||001 / 003": "08/2026"})

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_JA_PAGO.format(competencia="08/2026"))

    def test_blocks_duplicate_inside_upload(self):
        result = _enrich([_row(arquivo_origem="A.csv"), _row(arquivo_origem="B.csv")])

        self.assertEqual(result.loc[0, "status"], STATUS_APTO)
        self.assertEqual(result.loc[1, "status"], STATUS_BLOQUEADO_DUPLICADO)

    def test_blocks_parcela_without_rule(self):
        result = _enrich([_row(parcela="001 / 004")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_REGRA)

    def test_blocks_invalid_vl_bem(self):
        result = _enrich([_row(vl_bem=0.0), _row(cd_cota="0026", vl_bem=None)])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_VALOR)
        self.assertEqual(result.loc[1, "status"], STATUS_BLOQUEADO_VALOR)


class PremiacaoTests(unittest.TestCase):
    def test_builds_three_summary_tables(self):
        rows = _enrich(
            [
                _row(),
                _row(cd_cota="0026"),
                _row(cd_cota="0027", parcela="002 / 003", vl_bem=206994.15),
            ]
        )

        tables = build_premiacao_tables(rows)

        self.assertAlmostEqual(tables["cen"].loc[0, "Valor a pagar"], 3360.0, places=2)
        self.assertEqual(tables["cen"].loc[0, "CEN"], "VENDEDOR A")
        self.assertAlmostEqual(tables["gestor"].loc[0, "Valor a pagar"], 434.687715, places=6)
        self.assertEqual(tables["gestor"].loc[0, "Gestor"], "Reginaldo Pereira Garcia")
        self.assertAlmostEqual(tables["coordenador"].loc[0, "Valor a pagar"], 310.491225, places=6)


class SuggestionTests(unittest.TestCase):
    def test_suggests_by_exact_name_without_filling_cod_vendedor(self):
        vendedor_map = pd.DataFrame(
            [
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "Marcio Jose Louredo de Bessa", "cod_vendedor": ""},
                {"cd_vendedor_consorcio": "009999", "nm_vendedor_consorcio": "NOME QUE NAO EXISTE", "cod_vendedor": ""},
            ]
        )
        relations = pd.DataFrame(
            [{"cod_vendedor": "A0000E", "vendedor": "MARCIO JOSE LOUREDO DE BESSA", "gerente": "Hernane Borges da Costa"}]
        )

        suggestions = suggest_cod_vendedor(vendedor_map, relations)

        self.assertEqual(suggestions.loc[0, "sugestao_cod_vendedor"], "A0000E")
        self.assertEqual(suggestions.loc[0, "sugestao_cen"], "MARCIO JOSE LOUREDO DE BESSA")
        self.assertEqual(suggestions.loc[0, "cod_vendedor"], "")
        self.assertEqual(suggestions.loc[1, "sugestao_cod_vendedor"], "")


class ConsorcioAnalyticReportTests(unittest.TestCase):
    def test_renames_columns_to_labels_in_expected_order(self):
        raw = pd.DataFrame(
            [
                {
                    "id": 1,
                    "arquivo_origem": "arquivo.csv",
                    "cd_pedido": "12345",
                    "cd_grupo": "0301",
                    "cd_cota": "0025",
                    "versao": "0",
                    "parcela": "001 / 003",
                    "dt_aprovacao": "2026-09-10",
                    "consorciado": "FULANO DE TAL",
                    "vl_bem": 200000.0,
                    "cd_pdv": "208016",
                    "filial": "Loja Acreuna",
                    "cd_vendedor_consorcio": "002197",
                    "nm_vendedor_consorcio": "VENDEDOR A",
                    "cod_vendedor": "00188",
                    "cen": "VENDEDOR A",
                    "gerente": "Reginaldo Pereira Garcia",
                    "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
                    "percentual_cen": 0.84,
                    "valor_comissao_cen": 1680.0,
                    "percentual_gestor": 0.0,
                    "valor_comissao_gestor": 0.0,
                    "percentual_coordenador": 0.0,
                    "valor_comissao_coordenador": 0.0,
                    "competencia_ano": 2026,
                    "competencia_mes": 9,
                    "mes_ano_comissao": "09/2026",
                }
            ]
        )

        analytic = build_consorcio_analytic_df(raw)

        expected_columns = [label for _, label in CONSORCIO_ANALYTIC_COLUMN_MAP]
        self.assertEqual(list(analytic.columns), expected_columns)
        self.assertEqual(analytic.loc[0, "Filial"], "Loja Acreuna")
        self.assertEqual(analytic.loc[0, "Data de Aprovação"], "10/09/2026")
        self.assertEqual(analytic.loc[0, "Pedido"], "12345")
        self.assertEqual(analytic.loc[0, "Grupo"], "0301")
        self.assertEqual(analytic.loc[0, "Cota"], "0025")
        self.assertEqual(analytic.loc[0, "Versão"], "0")
        self.assertEqual(analytic.loc[0, "Consorciado"], "FULANO DE TAL")
        self.assertEqual(analytic.loc[0, "Parcela"], "001 / 003")
        self.assertEqual(analytic.loc[0, "Valor do Bem"], 200000.0)
        self.assertEqual(analytic.loc[0, "% Comissão CEN"], 0.84)
        self.assertEqual(analytic.loc[0, "Valor Comissão CEN"], 1680.0)
        self.assertEqual(analytic.loc[0, "CEN"], "VENDEDOR A")
        self.assertEqual(analytic.loc[0, "Gerente"], "Reginaldo Pereira Garcia")

    def test_drops_internal_database_columns(self):
        raw = pd.DataFrame(
            [
                {
                    "id": 1,
                    "arquivo_origem": "arquivo.csv",
                    "cod_vendedor": "00188",
                    "cd_vendedor_consorcio": "002197",
                    "nm_vendedor_consorcio": "VENDEDOR A",
                    "cd_pdv": "208016",
                    "competencia_ano": 2026,
                    "competencia_mes": 9,
                    "mes_ano_comissao": "09/2026",
                    "filial": "Loja Acreuna",
                    "cen": "VENDEDOR A",
                }
            ]
        )

        analytic = build_consorcio_analytic_df(raw)

        for internal_column in [
            "id",
            "arquivo_origem",
            "cod_vendedor",
            "cd_vendedor_consorcio",
            "nm_vendedor_consorcio",
            "cd_pdv",
            "competencia_ano",
            "competencia_mes",
            "mes_ano_comissao",
        ]:
            self.assertNotIn(internal_column, analytic.columns)

    def test_missing_source_column_becomes_empty_instead_of_raising(self):
        raw = pd.DataFrame([{"filial": "Loja Acreuna", "cen": "VENDEDOR A"}])

        analytic = build_consorcio_analytic_df(raw)

        self.assertEqual(analytic.loc[0, "Filial"], "Loja Acreuna")
        self.assertEqual(analytic.loc[0, "CEN"], "VENDEDOR A")
        self.assertTrue(pd.isna(analytic.loc[0, "Cota"]))

    def test_empty_input_returns_empty_frame_with_expected_columns(self):
        analytic = build_consorcio_analytic_df(pd.DataFrame())

        expected_columns = [label for _, label in CONSORCIO_ANALYTIC_COLUMN_MAP]
        self.assertEqual(list(analytic.columns), expected_columns)
        self.assertTrue(analytic.empty)

        analytic_none = build_consorcio_analytic_df(None)
        self.assertEqual(list(analytic_none.columns), expected_columns)
        self.assertTrue(analytic_none.empty)
