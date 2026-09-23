import unittest

import openpyxl
import pandas as pd

from commission_tool.io.consorcio_excel import build_consorcio_workbook


class ConsorcioWorkbookTests(unittest.TestCase):
    def test_workbook_has_base_and_premiacao_sheets(self):
        base = pd.DataFrame(
            [
                {
                    "filial": "Loja Acreuna",
                    "cen": "VENDEDOR A",
                    "cd_grupo": "002362",
                    "cd_cota": "0025",
                    "parcela": "001 / 003",
                    "vl_bem": 200000.0,
                    "valor_comissao_cen": 1680.0,
                }
            ]
        )
        premiacao = {
            "cen": pd.DataFrame([{"CEN": "VENDEDOR A", "Filial": "Loja Acreuna", "Valor a pagar": 1680.0}]),
            "gestor": pd.DataFrame([{"Gestor": "Reginaldo Pereira Garcia", "Valor a pagar": 0.0}]),
            "coordenador": pd.DataFrame(
                [{"Coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR", "Valor a pagar": 0.0}]
            ),
        }

        buffer = build_consorcio_workbook(base, premiacao)
        workbook = openpyxl.load_workbook(buffer)

        self.assertEqual(
            workbook.sheetnames,
            ["Base", "Premiação CEN", "Premiação Gestores", "Premiação Coordenador"],
        )
        self.assertEqual(workbook["Premiação CEN"].cell(2, 1).value, "VENDEDOR A")
