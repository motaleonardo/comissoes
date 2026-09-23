import unittest

import pandas as pd

from commission_tool.core.consorcio import (
    STATUS_APTO,
    STATUS_BLOQUEADO_DUPLICADO,
    STATUS_BLOQUEADO_GERENTE,
    STATUS_BLOQUEADO_JA_PAGO,
    STATUS_BLOQUEADO_PDV,
    STATUS_BLOQUEADO_REGRA,
    STATUS_BLOQUEADO_VALOR,
    STATUS_BLOQUEADO_VENDEDOR,
    STATUS_SEM_COMISSAO,
    build_premiacao_tables,
    enrich_consorcio_rows,
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
