import io
import unittest

from commission_tool.io.consorcio_csv import ConsorcioFileError, read_consorcio_csv


HEADER_ATUAL = (
    "CD_Pedido;NM_Produto;CD_Comissionado_PDV;NM_Comissionado_PDV;"
    "CD_Comissionado_Vendedor_Atual;NM_Comissionado_Vendedor_Atual;Consorciado;"
    "CD_Grupo;CD_Cota;Versao;ST_Contemplacao;CD_Periodo;DT_Alocacao;DT_Aprovacao;"
    "VL_Bem;Parcela;PE_Comissao;VL_Comissao"
)
HEADER_ORIGINAL = HEADER_ATUAL.replace("_Atual", "_Original")
LINHA = (
    "4504255710;MÁQ. AGRÍCOLAS;208016;Hohl Maquinas Agricolas Ltda;002197;"
    "VENDEDOR TESTE;CONSORCIADO TESTE;002362;0025;00;Não Contemplada;001;"
    "21/07/2026;27/07/2026;206994,15;001 / 003;1;2069,94"
)


def _file(header: str, *linhas: str, bom: bool = True) -> io.BytesIO:
    conteudo = "\r\n".join([header, *linhas]) + "\r\n"
    prefixo = "﻿" if bom else ""
    return io.BytesIO((prefixo + conteudo).encode("utf-8"))


class ConsorcioCsvTests(unittest.TestCase):
    def test_reads_layout_with_vendedor_atual(self):
        df = read_consorcio_csv(_file(HEADER_ATUAL, LINHA), file_name="Acreuna.csv")

        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "cd_pdv"], "208016")
        self.assertEqual(df.loc[0, "cd_vendedor_consorcio"], "002197")
        self.assertEqual(df.loc[0, "nm_vendedor_consorcio"], "VENDEDOR TESTE")
        self.assertEqual(df.loc[0, "cd_grupo"], "002362")
        self.assertEqual(df.loc[0, "cd_cota"], "0025")
        self.assertEqual(df.loc[0, "versao"], "00")
        self.assertEqual(df.loc[0, "parcela"], "001 / 003")
        self.assertAlmostEqual(df.loc[0, "vl_bem"], 206994.15, places=2)
        self.assertAlmostEqual(df.loc[0, "vl_comissao"], 2069.94, places=2)
        self.assertEqual(df.loc[0, "arquivo_origem"], "Acreuna.csv")

    def test_reads_layout_with_vendedor_original(self):
        df = read_consorcio_csv(_file(HEADER_ORIGINAL, LINHA), file_name="Goiania.csv")

        self.assertEqual(df.loc[0, "cd_vendedor_consorcio"], "002197")
        self.assertEqual(df.loc[0, "nm_vendedor_consorcio"], "VENDEDOR TESTE")

    def test_reads_file_without_bom(self):
        df = read_consorcio_csv(_file(HEADER_ATUAL, LINHA, bom=False), file_name="Sem BOM.csv")

        self.assertEqual(df.loc[0, "cd_pdv"], "208016")

    def test_rejects_file_missing_required_columns(self):
        header = HEADER_ATUAL.replace(";VL_Bem", "").replace(";Parcela", "")

        with self.assertRaises(ConsorcioFileError) as ctx:
            read_consorcio_csv(_file(header, ""), file_name="Quebrado.csv")

        self.assertIn("VL_Bem", ctx.exception.missing)
        self.assertIn("Parcela", ctx.exception.missing)

    def test_rejects_file_without_any_vendedor_column(self):
        header = HEADER_ATUAL.replace(
            "CD_Comissionado_Vendedor_Atual;NM_Comissionado_Vendedor_Atual;", ""
        )

        with self.assertRaises(ConsorcioFileError) as ctx:
            read_consorcio_csv(_file(header, ""), file_name="Sem vendedor.csv")

        self.assertIn("CD_Comissionado_Vendedor_Atual", ctx.exception.missing)
