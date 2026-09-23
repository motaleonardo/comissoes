import unittest

import pandas as pd

from commission_tool.ui.consorcio import prepare_pdv_editor_df, prepare_vendedor_editor_df


class ConsorcioEditorTests(unittest.TestCase):
    def test_pdv_editor_has_expected_columns(self):
        df = pd.DataFrame([{"cd_pdv": "2001", "nm_pdv": "HOHL GOIANIA", "filial": "Loja Goiania"}])

        prepared = prepare_pdv_editor_df(df)

        self.assertEqual(prepared.columns.tolist(), ["cd_pdv", "nm_pdv", "filial"])

    def test_vendedor_editor_lists_pending_first(self):
        vendedor_map = pd.DataFrame(
            [
                {"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A", "cod_vendedor": "00188"},
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B", "cod_vendedor": ""},
            ]
        )
        relations = pd.DataFrame(
            [{"cod_vendedor": "00188", "vendedor": "VENDEDOR A", "gerente": "Reginaldo Pereira Garcia"}]
        )

        prepared = prepare_vendedor_editor_df(vendedor_map, relations)

        self.assertEqual(prepared["cd_vendedor_consorcio"].tolist(), ["002697", "002197"])
        self.assertEqual(
            prepared.columns.tolist(),
            [
                "cd_vendedor_consorcio",
                "nm_vendedor_consorcio",
                "cod_vendedor",
                "sugestao_cod_vendedor",
                "sugestao_cen",
            ],
        )
