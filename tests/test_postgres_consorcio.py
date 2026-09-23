import unittest
from unittest.mock import patch

import pandas as pd

from commission_tool.data.sources.postgres_consorcio import (
    CONSORCIO_PDV_TABLE,
    CONSORCIO_VENDEDOR_TABLE,
    _prepare_pdv_map,
    _prepare_vendedor_map,
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


if __name__ == "__main__":
    unittest.main()
