"""SQL Server access for BDN tables.

Keep SQL statements here so validation and calculation modules can remain
focused on business rules.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import pyodbc


def get_connection(
    server: str,
    database: str,
    username: str = "",
    password: str = "",
    use_windows_auth: bool = False,
):
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    trust_cert = os.getenv("DB_TRUST_SERVER_CERTIFICATE", "no")
    encrypt = os.getenv("DB_ENCRYPT", "yes")

    if use_windows_auth:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            f"TrustServerCertificate={trust_cert};Encrypt={encrypt};"
        )
    else:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password};"
            f"TrustServerCertificate={trust_cert};Encrypt={encrypt};"
        )

    return pyodbc.connect(conn_str, timeout=10)


@dataclass(frozen=True)
class FaturamentoRecord:
    cliente_codigo: str
    data_emissao: Any
    nota_fiscal_numero: str


@dataclass(frozen=True)
class ReceivableSummary:
    saldo_total: float
    data_emissao: Any


@dataclass(frozen=True)
class ReceivableByType:
    tipo_titulo: str
    saldo: float


class SQLServerDataSource:
    """Repository-style adapter for BDN tables used by the tool."""

    def __init__(self, conn):
        self.conn = conn

    def find_incentive_invoice_by_chassi(self, chassi: str) -> str | None:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT TOP 1 [Nota Fiscal Número] "
            "FROM bdnIncentivos "
            "WHERE LTRIM(RTRIM([Chassi])) = ?",
            chassi,
        )
        row = cursor.fetchone()
        return str(row[0]).strip() if row else None

    def get_receivable_summary_by_title(self, titulo: str) -> ReceivableSummary | None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                SUM([Valor Saldo]) AS saldo_total,
                MIN([Data de Emissão]) AS data_emissao
            FROM bdnContasReceber
            WHERE [Título Número] = ?
            """,
            titulo,
        )
        row = cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return ReceivableSummary(saldo_total=float(row[0]), data_emissao=row[1])

    def find_invoice(
        self,
        document_variants: list[str],
        issue_date: date | None = None,
    ) -> FaturamentoRecord | None:
        cursor = self.conn.cursor()

        for doc_variant in document_variants:
            row = None
            if issue_date:
                cursor.execute(
                    """
                    SELECT TOP 1
                        [Cliente Código],
                        [Data de Emissão],
                        [Nota Fiscal Número]
                    FROM bdnFaturamento
                    WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?
                      AND [Data de Emissão] = ?
                    """,
                    (doc_variant, issue_date.strftime("%d/%m/%Y")),
                )
                row = cursor.fetchone()

            if not row:
                cursor.execute(
                    """
                    SELECT TOP 1
                        [Cliente Código],
                        [Data de Emissão],
                        [Nota Fiscal Número]
                    FROM bdnFaturamento
                    WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?
                    """,
                    (doc_variant,),
                )
                row = cursor.fetchone()

            if row:
                return FaturamentoRecord(
                    cliente_codigo=str(row[0]).strip(),
                    data_emissao=row[1],
                    nota_fiscal_numero=str(row[2]).strip(),
                )

        return None

    def get_receivables_by_customer_title(
        self,
        cliente_codigo: str,
        titulo: str,
    ) -> list[ReceivableByType]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT [Tipo Título], SUM([Valor Saldo]) AS saldo_tipo
            FROM bdnContasReceber
            WHERE [Cliente Código] = ?
              AND [Título Número]  = ?
            GROUP BY [Tipo Título]
            """,
            (cliente_codigo, titulo),
        )
        return [
            ReceivableByType(
                tipo_titulo=str(row[0]).strip() if row[0] else "",
                saldo=float(row[1]) if row[1] is not None else 0.0,
            )
            for row in cursor.fetchall()
        ]

    def count_chassi(self, chassi: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM bdnIncentivos WHERE LTRIM(RTRIM([Chassi])) = ?",
            chassi,
        )
        return int(cursor.fetchone()[0])

    def count_document(self, document: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM bdnFaturamento "
            "WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?",
            document,
        )
        return int(cursor.fetchone()[0])

    def extract_incentive_titles_by_chassi(self) -> pd.DataFrame:
        """Extract incentive totals and receivable balances by chassis/title."""
        sql = """
        WITH incentivo_titulos AS (
            SELECT
                LTRIM(RTRIM([Chassi])) AS chassi,
                LTRIM(RTRIM([Nota Fiscal Número])) AS titulo_incentivo,
                SUM(COALESCE([Valor Incentivo], 0)) AS valor_incentivo
            FROM bdnIncentivos
            WHERE NULLIF(LTRIM(RTRIM([Chassi])), '') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM([Nota Fiscal Número])), '') IS NOT NULL
            GROUP BY
                LTRIM(RTRIM([Chassi])),
                LTRIM(RTRIM([Nota Fiscal Número]))
        ),
        contas_receber AS (
            SELECT
                LTRIM(RTRIM([Título Número])) AS titulo_incentivo,
                SUM(COALESCE([Valor Saldo], 0)) AS saldo_incentivo
            FROM bdnContasReceber
            GROUP BY LTRIM(RTRIM([Título Número]))
        )
        SELECT
            incentivo_titulos.chassi AS [Nro Chassi],
            incentivo_titulos.titulo_incentivo AS [Título Incentivo],
            incentivo_titulos.valor_incentivo AS [Valor Incentivo],
            COALESCE(contas_receber.saldo_incentivo, 0.0) AS [Saldo Incentivo],
            CASE
                WHEN contas_receber.titulo_incentivo IS NULL THEN 'VERIFICAR'
                WHEN ABS(COALESCE(contas_receber.saldo_incentivo, 0.0)) < 0.01 THEN 'APTO'
                ELSE 'NÃO APTO'
            END AS [Status Incentivo],
            CASE
                WHEN contas_receber.titulo_incentivo IS NULL THEN 'Título não encontrado em bdnContasReceber'
                WHEN ABS(COALESCE(contas_receber.saldo_incentivo, 0.0)) < 0.01 THEN 'Incentivo quitado'
                ELSE 'Saldo pendente no título de incentivo'
            END AS [Detalhe Incentivo]
        FROM incentivo_titulos
        LEFT JOIN contas_receber
            ON contas_receber.titulo_incentivo COLLATE SQL_Latin1_General_CP1_CI_AS
             = incentivo_titulos.titulo_incentivo COLLATE SQL_Latin1_General_CP1_CI_AS
        ORDER BY incentivo_titulos.chassi, incentivo_titulos.titulo_incentivo
        """
        return pd.read_sql(sql, self.conn)

    def extract_incentive_summary_by_chassi(self) -> pd.DataFrame:
        """Extract one incentive row per chassis with distinct titles and total balance."""
        sql = """
        WITH incentivo_titulos AS (
            SELECT
                LTRIM(RTRIM([Chassi])) AS chassi,
                LTRIM(RTRIM([Nota Fiscal Número])) AS titulo_incentivo,
                SUM(COALESCE([Valor Incentivo], 0)) AS valor_incentivo
            FROM bdnIncentivos
            WHERE NULLIF(LTRIM(RTRIM([Chassi])), '') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM([Nota Fiscal Número])), '') IS NOT NULL
            GROUP BY
                LTRIM(RTRIM([Chassi])),
                LTRIM(RTRIM([Nota Fiscal Número]))
        ),
        contas_receber AS (
            SELECT
                LTRIM(RTRIM([Título Número])) AS titulo_incentivo,
                SUM(COALESCE([Valor Saldo], 0)) AS saldo_incentivo
            FROM bdnContasReceber
            GROUP BY LTRIM(RTRIM([Título Número]))
        ),
        incentivo_com_saldo AS (
            SELECT
                incentivo_titulos.chassi,
                incentivo_titulos.titulo_incentivo,
                incentivo_titulos.valor_incentivo,
                COALESCE(contas_receber.saldo_incentivo, 0.0) AS saldo_incentivo,
                CASE WHEN contas_receber.titulo_incentivo IS NULL THEN 1 ELSE 0 END AS titulo_nao_encontrado
            FROM incentivo_titulos
            LEFT JOIN contas_receber
                ON contas_receber.titulo_incentivo COLLATE SQL_Latin1_General_CP1_CI_AS
                 = incentivo_titulos.titulo_incentivo COLLATE SQL_Latin1_General_CP1_CI_AS
        )
        SELECT
            chassi AS [Nro Chassi],
            STRING_AGG(titulo_incentivo, ' / ') WITHIN GROUP (ORDER BY titulo_incentivo) AS [Títulos Incentivo],
            COUNT(*) AS [Qtd. Títulos Incentivo],
            SUM(valor_incentivo) AS [Valor Incentivo],
            SUM(saldo_incentivo) AS [Saldo Incentivo],
            CASE
                WHEN SUM(titulo_nao_encontrado) > 0 THEN 'VERIFICAR'
                WHEN ABS(SUM(saldo_incentivo)) < 0.01 THEN 'APTO'
                ELSE 'NÃO APTO'
            END AS [Status Incentivo],
            CASE
                WHEN SUM(titulo_nao_encontrado) > 0 THEN 'Há título de incentivo não encontrado em bdnContasReceber'
                WHEN ABS(SUM(saldo_incentivo)) < 0.01 THEN 'Todos os títulos de incentivo estão quitados'
                ELSE 'Há saldo pendente em título(s) de incentivo'
            END AS [Detalhe Incentivo]
        FROM incentivo_com_saldo
        GROUP BY chassi
        """
        return pd.read_sql(sql, self.conn)

    def extract_machine_source_audit(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Audit source rows and gross revenue before enrichment joins."""
        sql = """
        WITH origem AS (
            SELECT
                CAST('Faturamento' AS varchar(20)) AS tipo,
                COUNT(*) AS qtd_linhas,
                SUM(CAST(COALESCE(f.[Valor Total], 0) AS float)) AS receita_bruta
            FROM bdnFaturamentoMaquinas f
            WHERE TRY_CONVERT(date, f.[Data de Emissão], 103) BETWEEN ? AND ?
              AND (
                  f.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(f.[Produto Código])) NOT LIKE '%CVD%'
              )

            UNION ALL

            SELECT
                CAST('Devolução' AS varchar(20)) AS tipo,
                COUNT(*) AS qtd_linhas,
                SUM(CAST(COALESCE(d.[Valor Total], 0) AS float)) AS receita_bruta
            FROM bdnDevolucaoMaquinas d
            WHERE TRY_CONVERT(date, d.[Data de Emissão], 103) BETWEEN ? AND ?
              AND (
                  d.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(d.[Produto Código])) NOT LIKE '%CVD%'
              )
        )
        SELECT
            tipo AS [Tipo],
            qtd_linhas AS [Qtd Linhas SQL],
            COALESCE(receita_bruta, 0.0) AS [Receita Bruta SQL]
        FROM origem

        UNION ALL

        SELECT
            CAST('Total' AS varchar(20)) AS [Tipo],
            SUM(qtd_linhas) AS [Qtd Linhas SQL],
            SUM(COALESCE(receita_bruta, 0.0)) AS [Receita Bruta SQL]
        FROM origem
        """
        return pd.read_sql(
            sql,
            self.conn,
            params=(start_date, end_date, start_date, end_date),
        )

    def extract_machine_incentive_audit(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Audit incentives for chassis present in the source machine extraction."""
        sql = """
        WITH base_chassis AS (
            SELECT LTRIM(RTRIM(f.[Chassi])) AS chassi
            FROM bdnFaturamentoMaquinas f
            WHERE TRY_CONVERT(date, f.[Data de Emissão], 103) BETWEEN ? AND ?
              AND (
                  f.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(f.[Produto Código])) NOT LIKE '%CVD%'
              )
              AND NULLIF(LTRIM(RTRIM(f.[Chassi])), '') IS NOT NULL

            UNION

            SELECT LTRIM(RTRIM(d.[Chassi])) AS chassi
            FROM bdnDevolucaoMaquinas d
            WHERE TRY_CONVERT(date, d.[Data de Emissão], 103) BETWEEN ? AND ?
              AND (
                  d.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(d.[Produto Código])) NOT LIKE '%CVD%'
              )
              AND NULLIF(LTRIM(RTRIM(d.[Chassi])), '') IS NOT NULL
        ),
        incentivo_titulos AS (
            SELECT
                LTRIM(RTRIM(i.[Chassi])) AS chassi,
                NULLIF(LTRIM(RTRIM(i.[Nota Fiscal Número])), '') AS titulo_incentivo,
                SUM(CAST(COALESCE(i.[Valor Incentivo], 0) AS float)) AS valor_incentivo
            FROM bdnIncentivos i
            INNER JOIN base_chassis b
                ON b.chassi COLLATE SQL_Latin1_General_CP1_CI_AS
                 = LTRIM(RTRIM(i.[Chassi])) COLLATE SQL_Latin1_General_CP1_CI_AS
            WHERE NULLIF(LTRIM(RTRIM(i.[Chassi])), '') IS NOT NULL
            GROUP BY
                LTRIM(RTRIM(i.[Chassi])),
                LTRIM(RTRIM(i.[Nota Fiscal Número]))
        )
        SELECT
            COUNT(DISTINCT b.chassi) AS [Qtd Chassis SQL],
            COUNT(DISTINCT incentivo_titulos.chassi) AS [Qtd Chassis com Incentivo SQL],
            COUNT(DISTINCT incentivo_titulos.titulo_incentivo) AS [Qtd Títulos Incentivo SQL],
            COALESCE(SUM(incentivo_titulos.valor_incentivo), 0.0) AS [Valor Incentivo SQL],
            COALESCE(
                SUM(
                    CASE
                        WHEN incentivo_titulos.titulo_incentivo IS NOT NULL
                        THEN incentivo_titulos.valor_incentivo
                        ELSE 0.0
                    END
                ),
                0.0
            ) AS [Valor Incentivo com Título SQL],
            COALESCE(
                SUM(
                    CASE
                        WHEN incentivo_titulos.titulo_incentivo IS NULL
                        THEN incentivo_titulos.valor_incentivo
                        ELSE 0.0
                    END
                ),
                0.0
            ) AS [Valor Incentivo sem Título SQL]
        FROM base_chassis b
        LEFT JOIN incentivo_titulos
            ON incentivo_titulos.chassi COLLATE SQL_Latin1_General_CP1_CI_AS
             = b.chassi COLLATE SQL_Latin1_General_CP1_CI_AS
        """
        return pd.read_sql(
            sql,
            self.conn,
            params=(start_date, end_date, start_date, end_date),
        )

    def extract_machine_commission_base(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Extract the unified machine billing/return table for commission work."""
        sql = """
        WITH incentivo_titulos AS (
            SELECT
                LTRIM(RTRIM([Chassi])) AS chassi,
                LTRIM(RTRIM([Nota Fiscal Número])) AS titulo_incentivo,
                SUM(COALESCE([Valor Incentivo], 0)) AS valor_incentivo
            FROM bdnIncentivos
            WHERE NULLIF(LTRIM(RTRIM([Chassi])), '') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM([Nota Fiscal Número])), '') IS NOT NULL
            GROUP BY
                LTRIM(RTRIM([Chassi])),
                LTRIM(RTRIM([Nota Fiscal Número]))
        ),
        incentivos AS (
            SELECT
                chassi,
                SUM(valor_incentivo) AS valor_incentivo
            FROM incentivo_titulos
            GROUP BY chassi
        ),
        veiculos AS (
            SELECT
                LTRIM(RTRIM([Chassi])) AS chassi,
                MAX(LTRIM(RTRIM([Veículo Modelo]))) AS veiculo_modelo,
                MAX(LTRIM(RTRIM([Veículo Categoria]))) AS veiculo_categoria,
                MAX(LTRIM(RTRIM([Veículo Estado]))) AS veiculo_estado
            FROM bdnVeiculos
            WHERE NULLIF(LTRIM(RTRIM([Chassi])), '') IS NOT NULL
            GROUP BY LTRIM(RTRIM([Chassi]))
        ),
        vendedores AS (
            SELECT
                LTRIM(RTRIM([Vendedor Código])) AS vendedor_codigo,
                MAX(LTRIM(RTRIM([Vendedor Nome]))) AS vendedor_nome
            FROM bdnVendedor
            WHERE NULLIF(LTRIM(RTRIM([Vendedor Código])), '') IS NOT NULL
            GROUP BY LTRIM(RTRIM([Vendedor Código]))
        ),
        clientes AS (
            SELECT
                LTRIM(RTRIM([Cliente Código])) AS cliente_codigo,
                MAX(LTRIM(RTRIM([Cliente Nome]))) AS cliente_nome
            FROM bdnCliente
            WHERE NULLIF(LTRIM(RTRIM([Cliente Código])), '') IS NOT NULL
            GROUP BY LTRIM(RTRIM([Cliente Código]))
        ),
        centros_custo AS (
            SELECT
                LTRIM(RTRIM(CAST([Código C.Custo] AS varchar(50)))) AS centro_custo,
                MAX(LTRIM(RTRIM([Descrição C.Custo]))) AS descricao_custo,
                MAX(LTRIM(RTRIM([Lojas]))) AS lojas
            FROM datawarehouse.dbo.bdnOrganizacaoCentroDeCusto
            WHERE [Código C.Custo] IS NOT NULL
            GROUP BY LTRIM(RTRIM(CAST([Código C.Custo] AS varchar(50))))
        ),
        base AS (
            SELECT
                CAST('Faturamento' AS varchar(20)) AS tipo,
                LTRIM(RTRIM(f.[Centro de Custo])) AS centro_custo,
                f.[Data de Emissão] AS data_emissao,
                f.[Nota Fiscal Número] AS nro_documento,
                LTRIM(RTRIM(f.[Chassi])) AS chassi,
                LTRIM(RTRIM(f.[Cliente Código])) AS cliente_codigo,
                LTRIM(RTRIM(f.[Vendedor Código])) AS vendedor_codigo,
                CAST(COALESCE(f.[Valor Total], 0) AS float) AS receita_bruta,
                CAST(COALESCE(f.[Valor Venda Líquida], 0) AS float) AS receita_liquida,
                CAST(COALESCE(f.[Valor Custo], 0) AS float) AS cmv,
                CASE
                    WHEN CAST(COALESCE(f.[Valor Total], 0) AS float) >= 0
                        THEN CAST(COALESCE(f.[Valor Total], 0) AS float)
                           - CAST(COALESCE(f.[Valor Impostos], 0) AS float)
                           - CAST(COALESCE(f.[Valor Custo], 0) AS float)
                    ELSE (CAST(COALESCE(f.[Valor Total], 0) AS float) * -1)
                         + CAST(COALESCE(f.[Valor Impostos], 0) AS float)
                         + CAST(COALESCE(f.[Valor Custo], 0) AS float)
                END AS margem
            FROM bdnFaturamentoMaquinas f
            WHERE TRY_CONVERT(date, f.[Data de Emissão], 103) BETWEEN ? AND ?
              AND (
                  f.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(f.[Produto Código])) NOT LIKE '%CVD%'
              )

            UNION ALL

            SELECT
                CAST('Devolução' AS varchar(20)) AS tipo,
                LTRIM(RTRIM(d.[Centro de Custo])) AS centro_custo,
                d.[Data de Emissão] AS data_emissao,
                d.[Nota Fiscal Número] AS nro_documento,
                LTRIM(RTRIM(d.[Chassi])) AS chassi,
                LTRIM(RTRIM(d.[Cliente Código])) AS cliente_codigo,
                LTRIM(RTRIM(d.[Vendedor Código])) AS vendedor_codigo,
                CAST(COALESCE(d.[Valor Total], 0) AS float) AS receita_bruta,
                (
                    CAST(COALESCE(d.[Valor Total], 0) AS float) * -1
                    - CAST(COALESCE(d.[Valor Impostos], 0) AS float)
                ) AS receita_liquida,
                CAST(COALESCE(d.[Valor Custo], 0) AS float) AS cmv,
                CASE
                    WHEN CAST(COALESCE(d.[Valor Total], 0) AS float) >= 0
                        THEN CAST(COALESCE(d.[Valor Total], 0) AS float)
                           - CAST(COALESCE(d.[Valor Impostos], 0) AS float)
                           - CAST(COALESCE(d.[Valor Custo], 0) AS float)
                    ELSE (CAST(COALESCE(d.[Valor Total], 0) AS float) * -1)
                         + CAST(COALESCE(d.[Valor Impostos], 0) AS float)
                         + CAST(COALESCE(d.[Valor Custo], 0) AS float)
                END AS margem
            FROM bdnDevolucaoMaquinas d
            WHERE TRY_CONVERT(date, d.[Data de Emissão], 103) BETWEEN ? AND ?
              AND (
                  d.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(d.[Produto Código])) NOT LIKE '%CVD%'
              )
        )
        SELECT
            base.tipo AS [Tipo],
            org.lojas AS [Filial],
            base.data_emissao AS [Data de Emissão],
            base.nro_documento AS [Nro Documento],
            veic.veiculo_modelo AS [Modelo],
            veic.veiculo_categoria AS [Veículo Categoria],
            veic.veiculo_estado AS [Veículo Estado],
            base.chassi AS [Nro Chassi],
            base.cliente_codigo AS [Cliente Código],
            cli.cliente_nome AS [Nome do Cliente],
            vend.vendedor_nome AS [CEN],
            base.vendedor_codigo AS [Cod Vendedor],
            CASE
                WHEN UPPER(LTRIM(RTRIM(COALESCE(veic.veiculo_categoria, '')))) = 'IP'
                    THEN 'Implemento'
                WHEN UPPER(LTRIM(RTRIM(COALESCE(veic.veiculo_estado, '')))) = 'NOVO'
                    THEN 'Maquinas JD - Novos'
                WHEN UPPER(LTRIM(RTRIM(COALESCE(veic.veiculo_estado, '')))) = 'USADO'
                    THEN 'Maquinas JD - Usados'
                ELSE NULL
            END AS [Classificação Venda],
            base.receita_bruta AS [Receita Bruta],
            CAST(0.0 AS float) AS [% Comissão Fat.],
            base.receita_bruta * 0.0 AS [Valor Comissão Fat.],
            base.cmv AS [CMV],
            base.margem AS [Margem R$],
            CASE
                WHEN NULLIF(base.receita_liquida, 0) IS NULL THEN 0.0
                ELSE (base.margem / base.receita_liquida) * 100
            END AS [% Margem Direta],
            COALESCE(inc.valor_incentivo, 0.0) AS [Valor Incentivo],
            base.receita_bruta + COALESCE(inc.valor_incentivo, 0.0) AS [Receita Bruta + Incentivos R$],
            base.margem + COALESCE(inc.valor_incentivo, 0.0) AS [Margem + Incentivos R$],
            CAST(0.0 AS float) AS [Meta de Margem],
            CASE
                WHEN NULLIF(base.receita_liquida, 0) IS NULL THEN 0.0
                ELSE ((base.margem + COALESCE(inc.valor_incentivo, 0.0)) / base.receita_liquida) * 100
            END AS [% Margem Bruta],
            CAST(0.0 AS float) AS [% Comissão Margem],
            base.receita_bruta * 0.0 AS [Valor Comissão Margem],
            (base.receita_bruta * 0.0) + (base.receita_bruta * 0.0) AS [Valor Comissão Total]
        FROM base
        LEFT JOIN incentivos inc
            ON inc.chassi COLLATE SQL_Latin1_General_CP1_CI_AS
             = base.chassi COLLATE SQL_Latin1_General_CP1_CI_AS
        LEFT JOIN veiculos veic
            ON veic.chassi COLLATE SQL_Latin1_General_CP1_CI_AS
             = base.chassi COLLATE SQL_Latin1_General_CP1_CI_AS
        LEFT JOIN vendedores vend
            ON vend.vendedor_codigo COLLATE SQL_Latin1_General_CP1_CI_AS
             = base.vendedor_codigo COLLATE SQL_Latin1_General_CP1_CI_AS
        LEFT JOIN clientes cli
            ON cli.cliente_codigo COLLATE SQL_Latin1_General_CP1_CI_AS
             = base.cliente_codigo COLLATE SQL_Latin1_General_CP1_CI_AS
        LEFT JOIN centros_custo org
            ON org.centro_custo COLLATE SQL_Latin1_General_CP1_CI_AS
             = base.centro_custo COLLATE SQL_Latin1_General_CP1_CI_AS
        ORDER BY
            TRY_CONVERT(date, base.data_emissao, 103),
            base.nro_documento,
            base.tipo
        """
        return pd.read_sql(
            sql,
            self.conn,
            params=(start_date, end_date, start_date, end_date),
        )
