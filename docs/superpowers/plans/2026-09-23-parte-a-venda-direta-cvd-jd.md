# Parte A — Venda Direta (CVD JD) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apurar o produto `CVD JD` como Venda Direta (6% para o CEN, 33% para o gerente) e impedir que uma linha sem chassi seja paga duas vezes.

**Architecture:** A extração SQL passa a aceitar `CVD JD`, traz a coluna `Produto Código` e classifica essas linhas como Venda Direta. O cálculo ganha uma regra por classificação, no mesmo padrão que já existe para Implemento e Usados. O confronto com o histórico pago passa a usar o documento como chave quando a linha não tem chassi.

**Tech Stack:** Python 3.9+, pandas, pyodbc (SQL Server), SQLAlchemy + psycopg (Postgres), Streamlit, unittest.

**Spec:** `docs/superpowers/specs/2026-09-22-venda-direta-e-consorcio-design.md` (Parte A)

## Global Constraints

- Rodar os testes com `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`. Todos os 80 testes atuais precisam continuar passando.
- Não usar conexão real com SQL Server nem com Postgres nos testes: usar `unittest.mock.patch`, como em `tests/test_sqlserver_machine_query.py` e `tests/test_postgres_mapping.py`.
- Nomes de coluna de negócio são acentuados e em português (`% Comissão Fat.`, `Produto Código`). Nos testes, escrever acentos como escape unicode quando o arquivo de teste já usa esse estilo (ex.: `[Produto Código]`).
- Somente o código de produto **exatamente** igual a `CVD JD` entra. `CVD`, `CVD JDT`, `CVD INCENTIVO`, `CVD CONSORICIO`, `CVD MAPFRE`, `CVD SOMPO` e `CVD SOMPO 02` continuam fora.
- Percentual da Venda Direta: **6,0** sobre a Receita Bruta. Sem comissão de margem, sem meta.
- Um commit por task, mensagem em português no padrão `feat:` / `fix:` / `test:`, terminando com a linha `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.

---

## File Structure

| Arquivo | Responsabilidade | Task |
|---|---|---|
| `commission_tool/data/sources/sqlserver.py` | Filtro de produto, coluna `Produto Código`, classificação Venda Direta | 1 |
| `commission_tool/core/apuracao.py` | Regra de 6% e chave de confronto condicional ao chassi | 2, 3 |
| `commission_tool/data/sources/postgres.py` | Resumos de histórico incluindo linhas sem chassi; coluna `produto_codigo` | 3, 4 |
| `commission_tool/core/reports.py` | `Produto Código` no analítico | 4 |
| `commission_tool/ui/app.py` | Coluna na tela e passagem do novo resumo | 3, 4 |
| `docs/regras_calculo.md` | Registro da regra | 4 |

---

### Task 1: Extração SQL aceita CVD JD e traz o produto

**Files:**
- Modify: `commission_tool/data/sources/sqlserver.py` (linhas 313, 327, 362, 374, 513, 545 do filtro; CTE `base` e SELECT de `extract_machine_commission_base`)
- Test: `tests/test_sqlserver_machine_query.py`

**Interfaces:**
- Consumes: nada.
- Produces: o DataFrame de `SQLServerDataSource.extract_machine_commission_base(start_date, end_date)` passa a ter a coluna `Produto Código` (str) e a classificação `Venda Direta` para linhas de `CVD JD`.

- [ ] **Step 1: Substituir o teste de exclusão por um de inclusão seletiva**

Em `tests/test_sqlserver_machine_query.py`, trocar o método `test_machine_query_excludes_cvd_product_codes` por:

```python
    def test_machine_query_includes_only_cvd_jd_product_code(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            SQLServerDataSource(conn=object()).extract_machine_commission_base(
                date(2026, 3, 16),
                date(2026, 4, 15),
            )

        sql = read_sql.call_args.args[0]

        self.assertIn("f.[Produto Código] IS NULL", sql)
        self.assertIn("LTRIM(RTRIM(f.[Produto Código])) NOT LIKE '%CVD%'", sql)
        self.assertIn("LTRIM(RTRIM(f.[Produto Código])) = 'CVD JD'", sql)
        self.assertIn("d.[Produto Código] IS NULL", sql)
        self.assertIn("LTRIM(RTRIM(d.[Produto Código])) NOT LIKE '%CVD%'", sql)
        self.assertIn("LTRIM(RTRIM(d.[Produto Código])) = 'CVD JD'", sql)
        self.assertNotIn("LIKE '%CVD JD%'", sql)

    def test_machine_query_selects_product_code_and_classifies_cvd_jd_as_venda_direta(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            SQLServerDataSource(conn=object()).extract_machine_commission_base(
                date(2026, 3, 16),
                date(2026, 4, 15),
            )

        sql = read_sql.call_args.args[0]

        self.assertIn("LTRIM(RTRIM(f.[Produto Código])) AS produto_codigo", sql)
        self.assertIn("LTRIM(RTRIM(d.[Produto Código])) AS produto_codigo", sql)
        self.assertIn("base.produto_codigo AS [Produto Código]", sql)
        self.assertIn("WHEN base.produto_codigo = 'CVD JD'", sql)
        self.assertIn("THEN 'Venda Direta'", sql)

    def test_audit_queries_include_cvd_jd(self):
        with patch(
            "commission_tool.data.sources.sqlserver.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            source = SQLServerDataSource(conn=object())
            source.extract_machine_source_audit(date(2026, 3, 16), date(2026, 4, 15))
            source.extract_machine_incentive_audit(date(2026, 3, 16), date(2026, 4, 15))

        for call in read_sql.call_args_list:
            sql = call.args[0]
            self.assertIn("= 'CVD JD'", sql)
```

Em `test_source_audit_counts_rows_and_gross_revenue_without_cvd`, renomear para `test_source_audit_counts_rows_and_gross_revenue` e trocar a linha `self.assertIn("NOT LIKE '%CVD%'", sql)` por `self.assertIn("= 'CVD JD'", sql)`.

- [ ] **Step 2: Rodar os testes e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_sqlserver_machine_query -v`
Expected: FAIL nos três métodos novos, com `AssertionError: "LTRIM(RTRIM(f.[Produto Código])) = 'CVD JD'" not found in ...`

- [ ] **Step 3: Ajustar o filtro nos seis pontos**

Em `commission_tool/data/sources/sqlserver.py`, cada um dos seis blocos de filtro passa de:

```sql
              AND (
                  f.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(f.[Produto Código])) NOT LIKE '%CVD%'
              )
```

para:

```sql
              AND (
                  f.[Produto Código] IS NULL
                  OR LTRIM(RTRIM(f.[Produto Código])) NOT LIKE '%CVD%'
                  OR LTRIM(RTRIM(f.[Produto Código])) = 'CVD JD'
              )
```

O mesmo vale para os blocos com alias `d.` (devolução). São seis ocorrências: linhas 313 e 327 (`extract_machine_source_audit`), 362 e 374 (`extract_machine_incentive_audit`), 513 e 545 (`extract_machine_commission_base`).

- [ ] **Step 4: Trazer o produto na CTE `base` e no SELECT**

Em `extract_machine_commission_base`, na primeira parte da CTE `base` (faturamento), acrescentar a coluna logo depois de `nro_documento`:

```sql
                f.[Nota Fiscal Número] AS nro_documento,
                LTRIM(RTRIM(f.[Produto Código])) AS produto_codigo,
```

E na parte da devolução:

```sql
                d.[Nota Fiscal Número] AS nro_documento,
                LTRIM(RTRIM(d.[Produto Código])) AS produto_codigo,
```

No SELECT final, logo depois de `base.nro_documento AS [Nro Documento],`:

```sql
            base.produto_codigo AS [Produto Código],
```

- [ ] **Step 5: Classificar CVD JD como Venda Direta**

No mesmo SELECT, o CASE da classificação ganha a nova condição como **primeira** (antes de IP/Novo/Usado, porque a maioria das linhas de CVD JD não tem registro em `bdnVeiculos`):

```sql
            CASE
                WHEN base.produto_codigo = 'CVD JD'
                    THEN 'Venda Direta'
                WHEN UPPER(LTRIM(RTRIM(COALESCE(veic.veiculo_categoria, '')))) = 'IP'
                    THEN 'Implemento'
                WHEN UPPER(LTRIM(RTRIM(COALESCE(veic.veiculo_estado, '')))) = 'NOVO'
                    THEN 'Maquinas JD - Novos'
                WHEN UPPER(LTRIM(RTRIM(COALESCE(veic.veiculo_estado, '')))) = 'USADO'
                    THEN 'Maquinas JD - Usados'
                ELSE NULL
            END AS [Classificação Venda],
```

- [ ] **Step 6: Rodar os testes e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_sqlserver_machine_query -v`
Expected: PASS em todos.

- [ ] **Step 7: Commit**

```bash
git add commission_tool/data/sources/sqlserver.py tests/test_sqlserver_machine_query.py
git commit -m "feat: extrai CVD JD como Venda Direta e traz Produto Codigo

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Regra de 6% para Venda Direta

**Files:**
- Modify: `commission_tool/core/apuracao.py` (constantes no topo e `apply_commission_rules`)
- Test: `tests/test_apuracao.py`

**Interfaces:**
- Consumes: coluna `Classificação Venda` com o valor `Venda Direta`, vinda da Task 1.
- Produces: `apply_commission_rules(df_machine, fat_rates, margin_rates)` devolve, para Venda Direta, `% Comissão Fat. = 6.0`, `Valor Comissão Fat. = Receita Bruta * 0.06`, `% Comissão Margem = 0.0`, `Meta de Margem = 0.0` e `Regra Classificação Aplicada = True`.

- [ ] **Step 1: Escrever o teste que falha**

Acrescentar em `tests/test_apuracao.py`, dentro de `ApuracaoTests`:

```python
    def test_venda_direta_uses_six_percent_and_ignores_margin_rules(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificacao Venda": "Venda Direta",
                    "Modelo": "",
                    "Receita Bruta": 100000.0,
                    "% Margem Bruta": 40.0,
                },
                {
                    "Classificacao Venda": "Venda Direta",
                    "Modelo": "6125J",
                    "Receita Bruta": 250000.0,
                    "% Margem Bruta": 40.0,
                },
            ]
        ).rename(columns={"Classificacao Venda": "Classificação Venda"})
        fat_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 9.0}])
        margin_rates = pd.DataFrame([{"modelo": "6125J", "percentual": 2.0, "meta_margem": 10.0}])

        result = apply_commission_rules(df_machine, fat_rates, margin_rates)

        self.assertEqual(result["% Comissão Fat."].tolist(), [6.0, 6.0])
        self.assertEqual(result["Valor Comissão Fat."].tolist(), [6000.0, 15000.0])
        self.assertEqual(result["% Comissão Margem"].tolist(), [0.0, 0.0])
        self.assertEqual(result["Meta de Margem"].tolist(), [0.0, 0.0])
        self.assertEqual(result["Valor Comissão Margem"].tolist(), [0.0, 0.0])
        self.assertEqual(result["Valor Comissão Total"].tolist(), [6000.0, 15000.0])
        self.assertEqual(result["Gatilho Comissão Margem Atingido"].tolist(), [False, False])
        self.assertTrue(result["Regra Classificação Aplicada"].all())

    def test_venda_direta_manager_keeps_thirty_three_percent(self):
        df_machine = pd.DataFrame(
            [
                {
                    "Classificação Venda": "Venda Direta",
                    "Cod Vendedor": "aaetbu",
                    "Valor Comissão Total": 6000.0,
                }
            ]
        )
        relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "AAETBU",
                    "gerente": "Clayton Monteiro de Castro Filho",
                    "percentual_comissao_gerente": 33.0,
                }
            ]
        )

        result = apply_manager_commission_rules(df_machine, relations)

        self.assertEqual(result.loc[0, "% Comissão Gerente"], 33.0)
        self.assertAlmostEqual(result.loc[0, "Valor Comissão Gerente"], 1980.0, places=2)
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_apuracao -v`
Expected: FAIL em `test_venda_direta_uses_six_percent_and_ignores_margin_rules` com `[0.0, 9.0] != [6.0, 6.0]`. O segundo teste já passa e serve de proteção contra regressão.

- [ ] **Step 3: Implementar a regra**

Em `commission_tool/core/apuracao.py`, junto das constantes do topo (depois de `USED_MACHINE_RATE = 1.0`):

```python
VENDA_DIRETA_RATE = 6.0
```

Em `apply_commission_rules`, logo depois de `used_machine_mask = normalized_classification.eq("MAQUINAS JD - USADOS")`:

```python
    venda_direta_mask = normalized_classification.eq("VENDA DIRETA")
    classification_override_mask = implemento_mask | used_machine_mask | venda_direta_mask
```

(a linha `classification_override_mask = implemento_mask | used_machine_mask` existente é substituída por essa)

E logo depois de `result.loc[used_machine_mask, "% Comissão Fat."] = USED_MACHINE_RATE`:

```python
    result.loc[venda_direta_mask, "% Comissão Fat."] = VENDA_DIRETA_RATE
```

As linhas seguintes, que zeram margem e meta e marcam `Regra Classificação Aplicada` para `classification_override_mask`, já cobrem a Venda Direta sem alteração.

- [ ] **Step 4: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_apuracao -v`
Expected: PASS em todos.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/core/apuracao.py tests/test_apuracao.py
git commit -m "feat: comissao de 6% para Venda Direta

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Trava de duplicidade para linhas sem chassi

**Files:**
- Modify: `commission_tool/data/sources/postgres.py:831-875` (`read_paid_commission_document_summary` e vizinhança)
- Modify: `commission_tool/core/apuracao.py` (`apply_paid_history_filter`, `prepare_machine_apuracao`)
- Modify: `commission_tool/ui/app.py` (`refresh_machine_apuracao_state`, por volta da linha 290)
- Test: `tests/test_apuracao.py`, `tests/test_postgres_mapping.py`

**Interfaces:**
- Consumes: `apply_paid_history_filter` da Task 2 (sem mudança de regra de cálculo).
- Produces:
  - `read_paid_commission_document_summary()` passa a devolver também linhas com `nro_chassi` vazio (documento sem chassi).
  - Nova `read_excluded_commission_document_summary() -> pd.DataFrame` com as colunas `nro_chassi`, `nro_documento`, `qtd_lancamentos_excluidos`.
  - `apply_paid_history_filter(df_machine, paid_summary, paid_document_summary=None, excluded_summary=None, excluded_document_summary=None)`.
  - `prepare_machine_apuracao(df_machine, fat_rates, margin_rates, paid_summary, paid_document_summary=None, manager_relations=None, excluded_summary=None, excluded_document_summary=None)`.

- [ ] **Step 1: Escrever o teste de confronto sem chassi**

Acrescentar em `tests/test_apuracao.py`:

```python
    def test_paid_history_blocks_document_without_chassi(self):
        df_machine = pd.DataFrame(
            [
                {"Nro Chassi": "", "Nro Documento": "000122580", "Valor Comissão Total": 100.0},
                {"Nro Chassi": "", "Nro Documento": "000999999", "Valor Comissão Total": 100.0},
                {"Nro Chassi": "CH1", "Nro Documento": "000122580", "Valor Comissão Total": 100.0},
            ]
        )
        paid_document_summary = pd.DataFrame(
            [
                {
                    "nro_chassi": "",
                    "nro_documento": "122580",
                    "qtd_lancamentos_documento_pago": 1,
                    "valor_comissao_total_documento": 100.0,
                }
            ]
        )

        candidates, full_history = apply_paid_history_filter(
            df_machine,
            paid_summary=None,
            paid_document_summary=paid_document_summary,
        )

        self.assertEqual(full_history["Bloqueado por Pagamento Histórico"].tolist(), [True, False, False])
        self.assertEqual(candidates["Nro Documento"].tolist(), ["000999999", "000122580"])

    def test_excluded_document_summary_blocks_row_without_chassi(self):
        df_machine = pd.DataFrame(
            [
                {"Nro Chassi": "", "Nro Documento": "000122580", "Valor Comissão Total": 100.0},
                {"Nro Chassi": "", "Nro Documento": "000999999", "Valor Comissão Total": 100.0},
            ]
        )
        excluded_document_summary = pd.DataFrame(
            [{"nro_chassi": "", "nro_documento": "122580", "qtd_lancamentos_excluidos": 1}]
        )

        candidates, full_history = apply_paid_history_filter(
            df_machine,
            paid_summary=None,
            excluded_document_summary=excluded_document_summary,
        )

        self.assertEqual(full_history["Bloqueado por Exclusão"].tolist(), [True, False])
        self.assertEqual(full_history.loc[0, "Status Confronto Pagas"], "Excluído manualmente")
        self.assertEqual(candidates["Nro Documento"].tolist(), ["000999999"])
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_apuracao -v`
Expected: FAIL — o primeiro teste falha porque hoje a linha sem chassi não é bloqueada; o segundo falha com `TypeError: apply_paid_history_filter() got an unexpected keyword argument 'excluded_document_summary'`.

- [ ] **Step 3: Aceitar o resumo de exclusões por documento em `apply_paid_history_filter`**

Em `commission_tool/core/apuracao.py`, a assinatura passa a ser:

```python
def apply_paid_history_filter(
    df_machine: pd.DataFrame,
    paid_summary: pd.DataFrame | None,
    paid_document_summary: pd.DataFrame | None = None,
    excluded_summary: pd.DataFrame | None = None,
    excluded_document_summary: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
```

Logo depois do bloco que trata `excluded_summary` (o que termina definindo `result["Bloqueado por Exclusão"]`), acrescentar:

```python
    if excluded_document_summary is None or excluded_document_summary.empty:
        result["Qtd Lançamentos Excluídos no Documento"] = 0
    else:
        excluded_doc = excluded_document_summary.copy()
        excluded_doc_chassi_col = _find_column(excluded_doc, ["nro_chassi", "Nro Chassi", "NRO_CHASSI"])
        excluded_doc_document_col = _find_column(
            excluded_doc,
            ["nro_documento", "Nro Documento", "NRO_DOCUMENTO"],
        )
        excluded_doc["__chassi_key"] = excluded_doc[excluded_doc_chassi_col].apply(normalize_chassi)
        excluded_doc["__document_key"] = excluded_doc[excluded_doc_document_col].apply(normalize_document)
        excluded_doc["__pair_key"] = excluded_doc["__chassi_key"] + "||" + excluded_doc["__document_key"]
        excluded_doc = excluded_doc.rename(
            columns={"qtd_lancamentos_excluidos": "Qtd Lançamentos Excluídos no Documento"}
        )
        if "Qtd Lançamentos Excluídos no Documento" not in excluded_doc.columns:
            excluded_doc["Qtd Lançamentos Excluídos no Documento"] = 1
        excluded_doc = excluded_doc[["__pair_key", "Qtd Lançamentos Excluídos no Documento"]].drop_duplicates(
            subset=["__pair_key"],
            keep="first",
        )
        result = result.merge(excluded_doc, on="__pair_key", how="left")

    result["Qtd Lançamentos Excluídos no Documento"] = pd.to_numeric(
        result["Qtd Lançamentos Excluídos no Documento"],
        errors="coerce",
    ).fillna(0)
    result["Bloqueado por Exclusão"] = result["Bloqueado por Exclusão"] | result[
        "Qtd Lançamentos Excluídos no Documento"
    ].gt(0)
```

Na lista `numeric_columns` que vem logo abaixo, acrescentar `"Qtd Lançamentos Excluídos no Documento"`.

- [ ] **Step 4: Repassar o parâmetro em `prepare_machine_apuracao`**

Na mesma `apuracao.py`:

```python
def prepare_machine_apuracao(
    df_machine: pd.DataFrame,
    fat_rates: pd.DataFrame | None,
    margin_rates: pd.DataFrame | None,
    paid_summary: pd.DataFrame | None,
    paid_document_summary: pd.DataFrame | None = None,
    manager_relations: pd.DataFrame | None = None,
    excluded_summary: pd.DataFrame | None = None,
    excluded_document_summary: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    calculated = apply_commission_rules(df_machine, fat_rates, margin_rates)
    calculated = apply_manager_commission_rules(calculated, manager_relations)
    return apply_paid_history_filter(
        calculated,
        paid_summary,
        paid_document_summary,
        excluded_summary,
        excluded_document_summary,
    )
```

- [ ] **Step 5: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_apuracao -v`
Expected: PASS. O primeiro teste passa porque o par `||122580` já era calculado; o que faltava era a origem dos dados (próximo step) e a exclusão por documento.

- [ ] **Step 6: Escrever o teste das consultas Postgres**

Acrescentar em `tests/test_postgres_mapping.py` (o import no topo passa a incluir `read_excluded_commission_document_summary` e `read_paid_commission_document_summary`):

```python
class DocumentSummaryQueryTests(unittest.TestCase):
    def _captured_sql(self, func):
        with patch("commission_tool.data.sources.postgres.ensure_commission_tables"), patch(
            "commission_tool.data.sources.postgres.get_engine",
            return_value=object(),
        ), patch(
            "commission_tool.data.sources.postgres.pd.read_sql",
            return_value=pd.DataFrame(),
        ) as read_sql:
            func()
        return str(read_sql.call_args.args[0])

    def test_paid_document_summary_keeps_rows_without_chassi(self):
        sql = self._captured_sql(read_paid_commission_document_summary)

        self.assertIn("COALESCE(UPPER(TRIM(nro_chassi)), '') AS nro_chassi", sql)
        self.assertIn("TRIM(nro_documento) <> ''", sql)
        self.assertNotIn("TRIM(nro_chassi) <> ''", sql)

    def test_excluded_document_summary_groups_by_chassi_and_document(self):
        sql = self._captured_sql(read_excluded_commission_document_summary)

        self.assertIn(EXCLUDED_COMMISSIONS_TABLE, sql)
        self.assertIn("COALESCE(UPPER(TRIM(nro_chassi)), '') AS nro_chassi", sql)
        self.assertIn("TRIM(nro_documento) AS nro_documento", sql)
        self.assertIn("COUNT(*) AS qtd_lancamentos_excluidos", sql)
```

- [ ] **Step 7: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_postgres_mapping -v`
Expected: FAIL com `ImportError: cannot import name 'read_excluded_commission_document_summary'`.

- [ ] **Step 8: Ajustar as consultas no Postgres**

Em `commission_tool/data/sources/postgres.py`, `read_paid_commission_document_summary` passa a ser:

```python
def read_paid_commission_document_summary() -> pd.DataFrame:
    """Read all paid commission history aggregated by chassis and document.

    Rows without chassis are kept: for them the document alone is the key.
    """
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT
            COALESCE(UPPER(TRIM(nro_chassi)), '') AS nro_chassi,
            TRIM(nro_documento) AS nro_documento,
            COUNT(*) AS qtd_lancamentos_documento_pago,
            COALESCE(SUM(valor_comissao_total), 0) AS valor_comissao_total_documento
        FROM {PAID_COMMISSIONS_TABLE}
        WHERE nro_documento IS NOT NULL
          AND TRIM(nro_documento) <> ''
        GROUP BY COALESCE(UPPER(TRIM(nro_chassi)), ''), TRIM(nro_documento)
        """
    )
    return pd.read_sql(query, engine)
```

E logo depois de `read_excluded_commission_chassis_summary`, acrescentar:

```python
def read_excluded_commission_document_summary() -> pd.DataFrame:
    """Read manually excluded commissions aggregated by chassis and document."""
    from sqlalchemy import text

    ensure_commission_tables()
    engine = get_engine()
    query = text(
        f"""
        SELECT
            COALESCE(UPPER(TRIM(nro_chassi)), '') AS nro_chassi,
            TRIM(nro_documento) AS nro_documento,
            COUNT(*) AS qtd_lancamentos_excluidos
        FROM {EXCLUDED_COMMISSIONS_TABLE}
        WHERE nro_documento IS NOT NULL
          AND TRIM(nro_documento) <> ''
        GROUP BY COALESCE(UPPER(TRIM(nro_chassi)), ''), TRIM(nro_documento)
        """
    )
    return pd.read_sql(query, engine)
```

- [ ] **Step 9: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_postgres_mapping -v`
Expected: PASS.

- [ ] **Step 10: Ligar na tela**

Em `commission_tool/ui/app.py`, em `refresh_machine_apuracao_state` (por volta da linha 290), acrescentar a leitura ao lado das outras (`read_excluded_commission_chassis_summary`), importando `read_excluded_commission_document_summary` no bloco de imports de `postgres`:

```python
    excluded_document_summary = read_excluded_commission_document_summary()
```

E a chamada de `prepare_machine_apuracao` passa a receber o novo argumento nomeado:

```python
        excluded_document_summary=excluded_document_summary,
```

Conferir, ao editar, que as chamadas existentes continuam usando argumentos nomeados. Se alguma estiver posicional, converter para nomeada antes de acrescentar.

- [ ] **Step 11: Rodar a suíte inteira**

Run: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`
Expected: OK, com os 80 testes anteriores mais os novos.

- [ ] **Step 12: Commit**

```bash
git add commission_tool/core/apuracao.py commission_tool/data/sources/postgres.py commission_tool/ui/app.py tests/test_apuracao.py tests/test_postgres_mapping.py
git commit -m "fix: bloqueia repagamento de documento sem chassi

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Persistir, exibir e documentar o Produto Código

**Files:**
- Modify: `commission_tool/data/sources/postgres.py` (`ensure_commission_tables`, `PAID_COLUMN_MAP`)
- Modify: `commission_tool/core/reports.py` (`ANALYTIC_REPORT_COLUMN_MAP`)
- Modify: `commission_tool/ui/app.py` (`format_machine_display_df`, por volta da linha 132)
- Modify: `docs/regras_calculo.md`
- Test: `tests/test_postgres_mapping.py`, `tests/test_reports.py`

**Interfaces:**
- Consumes: a coluna `Produto Código` do DataFrame da Task 1.
- Produces: `comissoespagas.produto_codigo` e `comissoesexcluidas.produto_codigo` gravados; `Produto Código` como coluna do analítico em `_build_analytic_report_df`.

- [ ] **Step 1: Escrever os testes**

Em `tests/test_postgres_mapping.py`, dentro de `PaidCommissionMappingTests`:

```python
    def test_prepare_paid_commissions_maps_product_code(self):
        df = pd.DataFrame(
            [
                {
                    "Tipo": "Faturamento",
                    "Filial": "Loja A",
                    "Nro Documento": "0001",
                    "Produto Código": "CVD JD",
                    "Classificação Venda": "Venda Direta",
                    "Valor Comissão Total": 6000.0,
                }
            ]
        )

        prepared = _prepare_paid_commissions_df(
            df,
            competence_year=2026,
            competence_month=9,
            period_label="09/2026",
            period_start=date(2026, 8, 16),
            period_end=date(2026, 9, 15),
            source="streamlit",
        )

        self.assertEqual(prepared.loc[0, "produto_codigo"], "CVD JD")
```

Em `tests/test_reports.py`, num teste novo dentro da classe que já cobre o analítico (usar o mesmo estilo dos testes existentes de `build_cen_analytic_reports`):

```python
    def test_analytic_report_includes_product_code(self):
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "09/2026",
                    "cod_vendedor": "AAETBU",
                    "cen": "ABNER SAMUEL FERREIRA COSTA",
                    "filial": "Loja Vianopolis",
                    "classificacao_venda": "Venda Direta",
                    "produto_codigo": "CVD JD",
                    "nro_documento": "000122580",
                    "valor_comissao_fat": 6000.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 6000.0,
                }
            ]
        )

        reports = build_cen_analytic_reports(paid, "09/2026")

        self.assertEqual(len(reports), 1)
        _, _, analytic = reports[0]
        self.assertIn("Produto Código", analytic.columns)
        self.assertEqual(analytic.loc[0, "Produto Código"], "CVD JD")
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_postgres_mapping tests.test_reports -v`
Expected: FAIL com `KeyError: 'produto_codigo'` e `AssertionError: 'Produto Código' not found in Index([...])`.

- [ ] **Step 3: Criar a coluna no banco e no mapa**

Em `commission_tool/data/sources/postgres.py`, no `CREATE TABLE IF NOT EXISTS {PAID_COMMISSIONS_TABLE}`, acrescentar `produto_codigo TEXT,` logo depois de `modelo TEXT,`. Fazer o mesmo no CREATE de `{EXCLUDED_COMMISSIONS_TABLE}`.

Ainda em `ensure_commission_tables`, dentro do mesmo `with engine.begin() as conn:`, acrescentar ao final as migrações para bases que já existem:

```python
        for table_name in (PAID_COMMISSIONS_TABLE, EXCLUDED_COMMISSIONS_TABLE):
            conn.execute(
                text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS produto_codigo TEXT")
            )
```

Em `PAID_COLUMN_MAP`, acrescentar depois de `"Modelo": "modelo",`:

```python
    "Produto Código": "produto_codigo",
```

- [ ] **Step 4: Levar a coluna ao analítico**

Em `commission_tool/core/reports.py`, em `ANALYTIC_REPORT_COLUMN_MAP`, acrescentar depois de `("nro_chassi", "Nro Chassi"),`:

```python
    ("produto_codigo", "Produto Código"),
```

- [ ] **Step 5: Mostrar na tela**

Em `commission_tool/ui/app.py`, na lista de colunas usada por `format_machine_display_df` (por volta da linha 132) e na configuração de colunas da tabela de apuração, incluir `"Produto Código"` logo depois de `"Nro Chassi"`. Seguir exatamente o padrão da coluna vizinha: se houver `column_config` com largura ou formatação, replicar como texto simples.

- [ ] **Step 6: Rodar a suíte inteira**

Run: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`
Expected: OK.

- [ ] **Step 7: Documentar a regra**

Em `docs/regras_calculo.md`, acrescentar ao final:

```markdown
## Venda Direta (CVD JD)

Vigente a partir de 09/2026, implementado em `commission_tool/core/apuracao.py`.

- Entram apenas as linhas de `bdnFaturamentoMaquinas` cujo `[Produto Código]` é exatamente `CVD JD`. Os demais códigos com `CVD` continuam fora da apuração.
- Classificação: `Venda Direta`, atribuída pelo código do produto, e não pelo cadastro do veículo (a maioria dessas linhas não tem chassi).
- Comissão do CEN: 6% sobre a Receita Bruta. Sem comissão de margem e sem meta.
- Comissão do gerente: 33% sobre a comissão do CEN, pela relação em `comissao_gerente_vendedor`.
- Confronto com o histórico: linha com chassi usa chassi + documento; linha sem chassi usa apenas o documento.
```

- [ ] **Step 8: Commit**

```bash
git add commission_tool/data/sources/postgres.py commission_tool/core/reports.py commission_tool/ui/app.py docs/regras_calculo.md tests/test_postgres_mapping.py tests/test_reports.py
git commit -m "feat: persiste e exibe Produto Codigo na apuracao

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Verificação final (manual, com o usuário)

Depois da Task 4, rodar a aplicação (`.venv/Scripts/streamlit run app.py`), extrair um período que contenha `CVD JD` (por exemplo 16/08/2026 a 15/09/2026) e conferir:

1. As linhas de `CVD JD` aparecem com classificação Venda Direta e 6%.
2. A coluna `Produto Código` aparece preenchida também nas linhas de máquinas.
3. Pagar uma linha sem chassi, atualizar a apuração e confirmar que ela volta como "Já tratado neste documento" em vez de "Novo para apuração".
