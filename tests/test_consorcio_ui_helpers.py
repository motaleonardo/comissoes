import unittest

import pandas as pd

from commission_tool.core.periods import build_commission_period
from commission_tool.ui.consorcio import (
    consorcio_period_label,
    prepare_pdv_editor_df,
    prepare_vendedor_editor_df,
)


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


class ConsorcioPeriodLabelTests(unittest.TestCase):
    def test_consorcio_period_label_matches_build_commission_period_format(self):
        """Regression for C1: the consortium apuração screen used to build its own
        "MM/YYYY" label (e.g. "08/2026") instead of the app-wide period label
        (e.g. "Agosto/2026"), so the read side (reports, e-mails) never matched what
        was written to comissoes_consorcio_pagas.mes_ano_comissao."""
        period = build_commission_period(2026, 7)

        self.assertEqual(period.label, "Agosto/2026")
        self.assertEqual(consorcio_period_label(period), period.label)
        self.assertNotEqual(consorcio_period_label(period), "07/2026")
        self.assertNotEqual(consorcio_period_label(period), "08/2026")
