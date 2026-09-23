import unittest
from unittest.mock import patch

import pandas as pd

from commission_tool.data.sources.postgres_consorcio import (
    CONSORCIO_PAID_TABLE,
    CONSORCIO_PDV_TABLE,
    CONSORCIO_VENDEDOR_TABLE,
    _prepare_consorcio_paid,
    _prepare_consorcio_regras,
    _prepare_pdv_map,
    _prepare_vendedor_map,
    read_consorcio_paid_keys,
    register_missing_consorcio_vendedores,
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


class PrepareMapTests(unittest.TestCase):
    def test_pdv_map_normalizes_code_without_leading_zeros(self):
        df = pd.DataFrame(
            [
                {"CD_Comissionado_PDV": "002001", "NM_Comissionado_PDV": "HOHL GOIANIA", "Filial": " Loja Goiania "},
                {"CD_Comissionado_PDV": "", "NM_Comissionado_PDV": "SEM CODIGO", "Filial": "Loja X"},
            ]
        )

        prepared = _prepare_pdv_map(df)

        self.assertEqual(prepared["cd_pdv"].tolist(), ["2001"])
        self.assertEqual(prepared["filial"].tolist(), ["Loja Goiania"])
        self.assertTrue(prepared["ativo"].all())

    def test_vendedor_map_keeps_blank_cod_vendedor(self):
        df = pd.DataFrame(
            [
                {"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A", "cod_vendedor": " 00188 "},
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B", "cod_vendedor": None},
            ]
        )

        prepared = _prepare_vendedor_map(df)

        self.assertEqual(prepared["cd_vendedor_consorcio"].tolist(), ["002197", "002697"])
        self.assertEqual(prepared["cod_vendedor"].tolist(), ["00188", ""])


class PrepareRegrasTests(unittest.TestCase):
    def test_keeps_real_percentages_unscaled_when_given_as_float(self):
        df = pd.DataFrame(
            [
                {
                    "parcela": "001 / 003",
                    "percentual_cen": 0.84,
                    "percentual_gestor": 0.0,
                    "percentual_coordenador": 0.0,
                },
                {
                    "parcela": "002 / 003",
                    "percentual_cen": 0.0,
                    "percentual_gestor": 0.21,
                    "percentual_coordenador": 0.15,
                },
            ]
        )

        prepared = _prepare_consorcio_regras(df)

        self.assertEqual(prepared["percentual_cen"].tolist(), [0.84, 0.0])
        self.assertEqual(prepared["percentual_gestor"].tolist(), [0.0, 0.21])
        self.assertEqual(prepared["percentual_coordenador"].tolist(), [0.0, 0.15])

    def test_keeps_parsing_comma_decimal_strings(self):
        df = pd.DataFrame(
            [
                {
                    "parcela": "001 / 003",
                    "percentual_cen": "0,84",
                    "percentual_gestor": "0,00",
                    "percentual_coordenador": "0,00",
                }
            ]
        )

        prepared = _prepare_consorcio_regras(df)

        self.assertEqual(prepared["percentual_cen"].tolist(), [0.84])


class RegisterMissingTests(unittest.TestCase):
    def test_registers_only_unknown_sellers_with_blank_cod_vendedor(self):
        conn = FakeConnection()
        rows = pd.DataFrame(
            [
                {"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A"},
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B"},
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B"},
            ]
        )
        known = pd.DataFrame(
            [{"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A", "cod_vendedor": "00188"}]
        )

        captured = {}

        def fake_to_sql(self, name, con, **kwargs):
            captured["name"] = name
            captured["df"] = self.copy()

        with patch("commission_tool.data.sources.postgres_consorcio.ensure_consorcio_tables"), patch(
            "commission_tool.data.sources.postgres_consorcio.get_engine",
            return_value=FakeEngine(conn),
        ), patch(
            "commission_tool.data.sources.postgres_consorcio.read_consorcio_vendedor_map",
            return_value=known,
        ), patch.object(pd.DataFrame, "to_sql", fake_to_sql):
            inserted = register_missing_consorcio_vendedores(rows)

        self.assertEqual(inserted, 1)
        self.assertEqual(captured["name"], CONSORCIO_VENDEDOR_TABLE)
        self.assertEqual(captured["df"]["cd_vendedor_consorcio"].tolist(), ["002697"])
        self.assertEqual(captured["df"]["cod_vendedor"].tolist(), [""])


class PreparePaidTests(unittest.TestCase):
    def test_keeps_only_apt_rows_and_expected_columns(self):
        df = pd.DataFrame(
            [
                {
                    "arquivo_origem": "Acreuna.csv",
                    "cd_pedido": "4504255710",
                    "cd_grupo": "002362",
                    "cd_cota": "0025",
                    "versao": "00",
                    "parcela": "001 / 003",
                    "st_contemplacao": "Não Contemplada",
                    "dt_alocacao": "21/07/2026",
                    "dt_aprovacao": "27/07/2026",
                    "consorciado": "CONSORCIADO TESTE",
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
                    "apto": True,
                },
                {
                    "arquivo_origem": "Acreuna.csv",
                    "cd_grupo": "002362",
                    "cd_cota": "0026",
                    "versao": "00",
                    "parcela": "003 / 003",
                    "vl_bem": 200000.0,
                    "apto": False,
                },
            ]
        )

        prepared = _prepare_consorcio_paid(df, 2026, 9, "09/2026")

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared.loc[0, "mes_ano_comissao"], "09/2026")
        self.assertEqual(prepared.loc[0, "competencia_ano"], 2026)
        self.assertEqual(prepared.loc[0, "competencia_mes"], 9)
        self.assertEqual(str(prepared.loc[0, "dt_alocacao"]), "2026-07-21")
        self.assertNotIn("apto", prepared.columns)
        self.assertNotIn("status", prepared.columns)

    def test_fails_closed_when_apto_column_is_missing(self):
        df = pd.DataFrame(
            [
                {
                    "arquivo_origem": "Acreuna.csv",
                    "cd_grupo": "002362",
                    "cd_cota": "0025",
                    "versao": "00",
                    "parcela": "001 / 003",
                    "vl_bem": 200000.0,
                }
            ]
        )

        prepared = _prepare_consorcio_paid(df, 2026, 9, "09/2026")

        self.assertEqual(len(prepared), 0)

    def test_missing_dt_alocacao_becomes_none(self):
        df = pd.DataFrame(
            [
                {
                    "cd_grupo": "002362",
                    "cd_cota": "0025",
                    "versao": "00",
                    "parcela": "001 / 003",
                    "apto": True,
                }
            ]
        )

        prepared = _prepare_consorcio_paid(df, 2026, 9, "09/2026")

        self.assertEqual(len(prepared), 1)
        self.assertIsNone(prepared.loc[0, "dt_alocacao"])

    def test_paid_keys_query_selects_key_columns(self):
        with patch("commission_tool.data.sources.postgres_consorcio.ensure_consorcio_tables"), patch(
            "commission_tool.data.sources.postgres_consorcio.get_engine",
            return_value=object(),
        ), patch(
            "commission_tool.data.sources.postgres_consorcio.pd.read_sql",
            return_value=pd.DataFrame(
                [
                    {
                        "cd_grupo": "002362",
                        "cd_cota": "0025",
                        "versao": "00",
                        "parcela": "001 / 003",
                        "mes_ano_comissao": "08/2026",
                    },
                    {
                        "cd_grupo": "002362",
                        "cd_cota": "0026",
                        "versao": "00",
                        "parcela": "001  /\t003",
                        "mes_ano_comissao": "08/2026",
                    },
                ]
            ),
        ) as read_sql:
            keys = read_consorcio_paid_keys()

        self.assertIn(CONSORCIO_PAID_TABLE, str(read_sql.call_args.args[0]))
        self.assertEqual(
            keys,
            {
                "002362||0025||00||001 / 003": "08/2026",
                "002362||0026||00||001 / 003": "08/2026",
            },
        )


if __name__ == "__main__":
    unittest.main()
