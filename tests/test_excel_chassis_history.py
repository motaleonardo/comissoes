import io
import unittest

import pandas as pd
from openpyxl import load_workbook

from commission_tool.io.excel import (
    build_chassis_history_workbook,
    sanitize_sheet_name,
)


class SanitizeSheetNameTests(unittest.TestCase):
    def test_replaces_forbidden_chars(self):
        used: set[str] = set()
        result = sanitize_sheet_name("ABC/123:XYZ*?[]\\", used)
        for forbidden in ["/", "\\", "?", "*", "[", "]", ":"]:
            self.assertNotIn(forbidden, result)

    def test_truncates_to_31_chars(self):
        used: set[str] = set()
        long_name = "A" * 60
        result = sanitize_sheet_name(long_name, used)
        self.assertLessEqual(len(result), 31)

    def test_handles_collisions_with_numeric_suffix(self):
        used: set[str] = set()
        first = sanitize_sheet_name("CHASSI001", used)
        second = sanitize_sheet_name("CHASSI001", used)
        self.assertNotEqual(first, second)
        self.assertTrue(second.endswith("_2"))

    def test_collision_truncation_keeps_suffix(self):
        used: set[str] = set()
        base = "X" * 31
        first = sanitize_sheet_name(base, used)
        second = sanitize_sheet_name(base, used)
        self.assertLessEqual(len(second), 31)
        self.assertTrue(second.endswith("_2"))
        self.assertNotEqual(first, second)

    def test_empty_falls_back_to_chassi(self):
        used: set[str] = set()
        self.assertEqual(sanitize_sheet_name("", used), "Chassi")


class BuildChassisHistoryWorkbookTests(unittest.TestCase):
    def test_creates_indice_first_then_one_sheet_per_chassis(self):
        history = {
            "AAAA111": pd.DataFrame(
                [{"nro_chassi": "AAAA111", "valor_comissao_total": 1000.0, "mes_ano_comissao": "MAI/2026"}]
            ),
            "BBBB222": pd.DataFrame(
                [
                    {"nro_chassi": "BBBB222", "valor_comissao_total": 500.0, "mes_ano_comissao": "ABR/2026"},
                    {"nro_chassi": "BBBB222", "valor_comissao_total": -500.0, "mes_ano_comissao": "MAI/2026"},
                ]
            ),
        }
        index_df = pd.DataFrame(
            [
                {"Nro Chassi": "AAAA111", "Lançamentos no Histórico": 1, "Valor Comissão Total Histórico": 1000.0},
                {"Nro Chassi": "BBBB222", "Lançamentos no Histórico": 2, "Valor Comissão Total Histórico": 0.0},
            ]
        )

        buf = build_chassis_history_workbook(history, index_df=index_df)
        wb = load_workbook(buf)

        self.assertEqual(wb.sheetnames[0], "Indice")
        self.assertIn("AAAA111", wb.sheetnames)
        self.assertIn("BBBB222", wb.sheetnames)
        self.assertEqual(wb["BBBB222"].max_row, 3)  # header + 2 rows

    def test_empty_history_still_produces_indice(self):
        buf = build_chassis_history_workbook({})
        wb = load_workbook(buf)
        self.assertEqual(wb.sheetnames, ["Indice"])


if __name__ == "__main__":
    unittest.main()
