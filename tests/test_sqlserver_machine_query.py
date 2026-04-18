import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from commission_tool.data.sources.sqlserver import SQLServerDataSource


class MachineCommissionQueryTests(unittest.TestCase):
    def test_machine_query_deduplicates_dimension_tables_before_joins(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            SQLServerDataSource(conn=object()).extract_machine_commission_base(
                date(2026, 3, 16),
                date(2026, 4, 15),
            )

        sql = read_sql.call_args.args[0]

        self.assertIn("veiculos AS (", sql)
        self.assertIn("MAX(LTRIM(RTRIM([Ve\u00edculo Categoria]))) AS veiculo_categoria", sql)
        self.assertIn("MAX(LTRIM(RTRIM([Ve\u00edculo Estado]))) AS veiculo_estado", sql)
        self.assertIn("vendedores AS (", sql)
        self.assertIn("clientes AS (", sql)
        self.assertIn("centros_custo AS (", sql)
        self.assertIn("GROUP BY LTRIM(RTRIM([Cliente C\u00f3digo]))", sql)
        self.assertIn("LEFT JOIN clientes cli", sql)
        self.assertNotIn("LEFT JOIN bdnCliente cli", sql)

    def test_machine_query_classifies_sale_from_vehicle_category_and_state(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            SQLServerDataSource(conn=object()).extract_machine_commission_base(
                date(2026, 3, 16),
                date(2026, 4, 15),
            )

        sql = read_sql.call_args.args[0]

        self.assertIn("veic.veiculo_categoria AS [Ve\u00edculo Categoria]", sql)
        self.assertIn("veic.veiculo_estado AS [Ve\u00edculo Estado]", sql)
        self.assertIn("= 'IP'", sql)
        self.assertIn("THEN 'Implemento'", sql)
        self.assertIn("= 'NOVO'", sql)
        self.assertIn("THEN 'Maquinas JD - Novos'", sql)
        self.assertIn("= 'USADO'", sql)
        self.assertIn("THEN 'Maquinas JD - Usados'", sql)

    def test_machine_query_excludes_cvd_product_codes(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            SQLServerDataSource(conn=object()).extract_machine_commission_base(
                date(2026, 3, 16),
                date(2026, 4, 15),
            )

        sql = read_sql.call_args.args[0]

        self.assertIn("f.[Produto C\u00f3digo] IS NULL", sql)
        self.assertIn("LTRIM(RTRIM(f.[Produto C\u00f3digo])) NOT LIKE '%CVD%'", sql)
        self.assertIn("d.[Produto C\u00f3digo] IS NULL", sql)
        self.assertIn("LTRIM(RTRIM(d.[Produto C\u00f3digo])) NOT LIKE '%CVD%'", sql)

    def test_source_audit_counts_rows_and_gross_revenue_without_cvd(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            SQLServerDataSource(conn=object()).extract_machine_source_audit(
                date(2026, 3, 16),
                date(2026, 4, 15),
            )

        sql = read_sql.call_args.args[0]

        self.assertIn("FROM bdnFaturamentoMaquinas f", sql)
        self.assertIn("FROM bdnDevolucaoMaquinas d", sql)
        self.assertIn("COUNT(*) AS qtd_linhas", sql)
        self.assertIn("SUM(CAST(COALESCE(f.[Valor Total], 0) AS float)) AS receita_bruta", sql)
        self.assertIn("SUM(CAST(COALESCE(d.[Valor Total], 0) AS float)) AS receita_bruta", sql)
        self.assertIn("NOT LIKE '%CVD%'", sql)

    def test_incentive_audit_sums_incentives_for_source_chassis(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            SQLServerDataSource(conn=object()).extract_machine_incentive_audit(
                date(2026, 3, 16),
                date(2026, 4, 15),
            )

        sql = read_sql.call_args.args[0]

        self.assertIn("base_chassis AS (", sql)
        self.assertIn("FROM bdnFaturamentoMaquinas f", sql)
        self.assertIn("FROM bdnDevolucaoMaquinas d", sql)
        self.assertIn("INNER JOIN base_chassis b", sql)
        self.assertIn("SUM(CAST(COALESCE(i.[Valor Incentivo], 0) AS float))", sql)
        self.assertIn("COUNT(DISTINCT b.chassi) AS [Qtd Chassis SQL]", sql)
        self.assertIn("AS [Valor Incentivo com T\u00edtulo SQL]", sql)
        self.assertIn("AS [Valor Incentivo sem T\u00edtulo SQL]", sql)


if __name__ == "__main__":
    unittest.main()
