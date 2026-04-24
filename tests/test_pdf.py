import unittest

import pandas as pd

from commission_tool.io.pdf import build_reports_pdf


class PdfTests(unittest.TestCase):
    def test_build_reports_pdf_returns_pdf_bytes_for_dataframe_sections(self):
        df = pd.DataFrame(
            [
                {"Coluna A": "Valor 1", "Coluna B": "Valor 2"},
                {"Coluna A": "Valor 3", "Coluna B": "Valor 4"},
            ]
        )

        pdf_bytes = build_reports_pdf(
            "Relatórios de Teste",
            [("Tabela Principal", df)],
        )

        self.assertTrue(pdf_bytes.startswith(b"%PDF"))
        self.assertGreater(len(pdf_bytes), 1000)


if __name__ == "__main__":
    unittest.main()
