# Travas de Cadastro, Incentivos sem Nota e CVD JD — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Somar incentivos sem nota na extração, dispensar `CVD JD` da validação do Contas a Receber e trocar o padrão de 1% por uma trava que lista todos os modelos sem regra e CENs sem gerente.

**Architecture:** SQL em `data/sources/sqlserver.py`; regra de validação em `core/eligibility.py`; identificação de pendências como função pura em `core/apuracao.py`; popup, aviso, navegação e "Reaplicar regras" em `ui/app.py`.

**Tech Stack:** Python, pandas, Streamlit 1.56 (`st.dialog`), pyodbc, unittest.

**Spec:** `docs/superpowers/specs/2026-11-25-travas-apuracao-incentivos-design.md`

Testes: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"` (não há pytest no venv).

## Global Constraints

- Só o código **exatamente** `CVD JD` (após `strip().upper()`) é dispensado; `CVD JDT`, `CVD INCENTIVO` etc. não.
- A trava olha só as linhas que ainda podem ser pagas: `machine_df` depois de `filter_old_emissions`.
- Falta de regra de **margem** não trava.
- Nada de padrão de 1%: modelo sem regra de faturamento nunca recebe percentual por suposição.
- Textos de tela em português do Brasil.

## Review Focus

1. Modelo vazio/NaN/só espaços em Máquinas Novas → aparece como `(sem modelo)` com chassi de exemplo, não some (teste na Task 3).
2. `Produto Código` `CVD JDT` ou `" cvd jd "` → o primeiro continua validado, o segundo é dispensado (teste na Task 2).
3. Relação CEN x Gerente com o código, mas `gerente` vazio → é pendência (teste na Task 3).
4. Apuração sem nenhuma linha (DataFrame vazio) → sem pendência e sem erro (teste na Task 3).
5. Desligar o filtro de 01/12/2025 traz linhas novas sem regra → a trava aparece (verificação manual na Task 4, a conferência roda após o filtro a cada render).

---

### Task 1: Incentivos sem nota na extração e busca de nota do incentivo

**Files:**
- Modify: `commission_tool/data/sources/sqlserver.py:82-91` (`find_incentive_invoice_by_chassi`) e `:439-450` (CTE `incentivo_titulos` de `extract_machine_commission_base`)
- Test: `tests/test_sqlserver_machine_query.py`

**Interfaces:**
- Produces: `find_incentive_invoice_by_chassi(chassi) -> str | None` passa a ignorar linhas com nota vazia/nula. `count_chassi(chassi) -> int` (já existe) segue contando todas as linhas do chassi.

- [ ] **Step 1: Write the failing tests**

```python
def test_machine_query_sums_incentives_without_invoice_number(self):
    # mesmo patch de pd.read_sql dos testes vizinhos
    sql = ...  # SQL de extract_machine_commission_base
    cte = sql.split("incentivos AS (")[0]
    self.assertNotIn("NULLIF(LTRIM(RTRIM([Nota Fiscal Número])), '') IS NOT NULL", cte)
    self.assertIn("NULLIF(LTRIM(RTRIM([Chassi])), '') IS NOT NULL", cte)

def test_find_incentive_invoice_ignores_rows_without_invoice_number(self):
    cursor = MagicMock(); cursor.fetchone.return_value = None
    conn = MagicMock(); conn.cursor.return_value = cursor
    result = SQLServerDataSource(conn).find_incentive_invoice_by_chassi("ABC123")
    executed_sql = cursor.execute.call_args.args[0]
    self.assertIn("NULLIF(LTRIM(RTRIM([Nota Fiscal Número])), '') IS NOT NULL", executed_sql)
    self.assertIsNone(result)
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv/Scripts/python -m unittest tests.test_sqlserver_machine_query -v`
Expected: FAIL nos dois testes novos.

- [ ] **Step 3: Implement**

Remover a condição de nota do `WHERE` do CTE `incentivo_titulos` em `extract_machine_commission_base`, mantendo o `GROUP BY` (nota nula vira um grupo próprio, somado em `incentivos`). Em `find_incentive_invoice_by_chassi`, acrescentar `AND NULLIF(LTRIM(RTRIM([Nota Fiscal Número])), '') IS NOT NULL`. `extract_incentive_titles_by_chassi` e `extract_incentive_summary_by_chassi` **não mudam**.

- [ ] **Step 4: Run all tests**

Run: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`
Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/data/sources/sqlserver.py tests/test_sqlserver_machine_query.py
git commit -m "feat: soma incentivos sem nota fiscal na extracao de maquinas"
```

---

### Task 2: CVD JD dispensado e V1 com incentivo sem nota

**Files:**
- Modify: `commission_tool/core/eligibility.py` (`validate_incentive`, `run_eligibility_validation`)
- Test: `tests/test_eligibility.py` (acrescentar `count_chassi` ao `FakeSource`, com `self.incentive_rows = {}`)

**Interfaces:**
- Consumes: `source.find_incentive_invoice_by_chassi`, `source.count_chassi` (Task 1).
- Produces: constante `VENDA_DIRETA_PRODUTO = "CVD JD"` em `core/eligibility.py`.

- [ ] **Step 1: Write the failing tests**

```python
def test_cvd_jd_is_apto_without_querying_receivables(self):
    df = pd.DataFrame([{"Produto Código": " cvd jd ", "Classificação Venda": "Venda Direta",
                        "Nro Documento": "123", "Nro Chassi": "", "Data de Emissão": "01/11/2026"}])
    source = FakeSource()  # sem faturas: se consultasse, daria NÃO ENCONTRADO
    with patch("commission_tool.core.eligibility.SQLServerDataSource", return_value=source):
        result = run_eligibility_validation(object(), df)
    row = result.iloc[0]
    self.assertEqual(row["Status Geral"], STATUS_APTO)
    self.assertEqual(row["V1 - Status"], "N/A")
    self.assertEqual(row["V2 - Status"], "N/A")
    self.assertEqual(row["V2 - Detalhe"], "Venda Direta (CVD JD) dispensada da validação")

def test_cvd_jdt_is_still_validated(self):
    # mesmo df com "Produto Código": "CVD JDT" → V2 - Status == "NÃO ENCONTRADO"

def test_incentive_without_invoice_number_goes_to_verify(self):
    source = FakeSource(); source.incentive_rows["CH1"] = 2  # sem entrada em incentive_nf
    result = validate_incentive(source, "CH1")
    self.assertEqual(result["v1_status"], "NÃO ENCONTRADO")
    self.assertEqual(result["v1_detalhe"], "Incentivo sem nota fiscal em bdnIncentivos")

def test_incentive_absent_keeps_old_detail(self):
    # count_chassi == 0 → v1_detalhe == "Chassi não localizado em bdnIncentivos"
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv/Scripts/python -m unittest tests.test_eligibility -v`
Expected: FAIL nos testes novos (exceto `test_cvd_jdt_is_still_validated`, que já passa).

- [ ] **Step 3: Implement**

Em `run_eligibility_validation`, antes de V1/V2: se `str(row.get("Produto Código", "")).strip().upper() == VENDA_DIRETA_PRODUTO`, montar v1/v2 com status `"N/A"`, valores `None` e detalhe `"Venda Direta (CVD JD) dispensada da validação"` nos dois, sem chamar o `source`. `combine_eligibility_status("N/A","N/A")` já dá APTO. Em `validate_incentive`, quando a nota não vier: `source.count_chassi(chassi_str) > 0` → detalhe `"Incentivo sem nota fiscal em bdnIncentivos"`; senão o detalhe atual.

- [ ] **Step 4: Run all tests** — mesmo comando da Task 1, Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/core/eligibility.py tests/test_eligibility.py
git commit -m "feat: dispensa CVD JD da validacao do contas a receber"
```

---

### Task 3: Identificar pendências de cadastro e remover o padrão de 1%

**Files:**
- Modify: `commission_tool/core/apuracao.py` (nova função; remover `apply_frontend_default_fat_commission`, linhas 291-348)
- Modify: `tests/test_apuracao.py` (remover os testes do padrão de 1%, ~linhas 270-330, e o import)
- Modify: `docs/regras_calculo.md` (seção nova "Travas de cadastro")

**Interfaces:**
- Consumes: colunas `Regra Comissão Fat. Encontrada`, `Regra Gerente Encontrada`, `Modelo`, `Classificação Venda`, `Nro Chassi`, `Cod Vendedor`, `CEN`, `Filial` (produzidas por `prepare_machine_apuracao`).
- Produces:
  - `PENDING_MODEL_COLUMNS = ["Modelo", "Classificação Venda", "Qtd Linhas", "Chassi Exemplo"]`
  - `PENDING_CEN_COLUMNS = ["Cod Vendedor", "CEN", "Filial", "Qtd Linhas"]`
  - `find_pending_registrations(df_machine: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]` → `(pendentes_modelo, pendentes_cen)`, sempre com essas colunas, ordenados por `Modelo` / `CEN`.

- [ ] **Step 1: Write the failing tests**

```python
def test_pending_lists_models_without_fat_rule_grouped(self):
    df = prepare_machine_apuracao(raw_df_with(
        [("Maquinas JD - Novos", "6110J", "CH1", "001"), ("Maquinas JD - Novos", "6110J", "CH2", "001"),
         ("Maquinas JD - Novos", "  ", "CH3", "001"), ("Implemento", "XYZ", "CH4", "001")]),
        fat_rates=pd.DataFrame(), margin_rates=None, paid_summary=None,
        manager_relations=pd.DataFrame([{"cod_vendedor": "001", "gerente": "G1"}]))[0]
    models, cens = find_pending_registrations(df)
    self.assertEqual(list(models.columns), PENDING_MODEL_COLUMNS)
    self.assertEqual(models["Modelo"].tolist(), ["(sem modelo)", "6110J"])  # ordenado; Implemento fora
    self.assertEqual(models.set_index("Modelo").loc["6110J", "Qtd Linhas"], 2)
    self.assertEqual(models.set_index("Modelo").loc["(sem modelo)", "Chassi Exemplo"], "CH3")
    self.assertTrue(cens.empty)

def test_pending_lists_cen_without_manager_and_blank_manager(self):
    # relações: {"001": "G1"}, {"002": ""}; linhas com Cod Vendedor "001", "002", "003", ""
    # → cens["Cod Vendedor"] contém "002", "003", "(sem código)"; não contém "001"

def test_pending_on_empty_dataframe(self):
    models, cens = find_pending_registrations(pd.DataFrame())
    self.assertTrue(models.empty and cens.empty)
    self.assertEqual(list(cens.columns), PENDING_CEN_COLUMNS)
```

(`raw_df_with` é um helper local do teste que monta o DataFrame mínimo com `Classificação Venda`, `Modelo`, `Nro Chassi`, `Cod Vendedor`, `CEN`, `Filial`, `Receita Bruta`, `Nro Documento`.)

- [ ] **Step 2: Run to verify they fail**

Run: `.venv/Scripts/python -m unittest tests.test_apuracao -v`
Expected: FAIL com ImportError de `find_pending_registrations`.

- [ ] **Step 3: Implement** `find_pending_registrations` com `groupby` (modelo vazio → `"(sem modelo)"`, código vazio → `"(sem código)"`, `Chassi Exemplo` = primeiro chassi não vazio). Remover `apply_frontend_default_fat_commission` e seus testes. Em `docs/regras_calculo.md`, registrar: sem padrão de 1%; trava por regra de faturamento e por gerente, só em linhas pagáveis; margem não trava; CVD JD dispensado da validação; incentivos sem nota somam no `Valor Incentivo`.

- [ ] **Step 4: Run all tests** — Expected: OK (o import de `apply_frontend_default_fat_commission` em `ui/app.py` só é removido na Task 4; `ui/app.py` não é importado pelos testes de `apuracao`. Se algum teste importar `ui/app.py`, remover o import já nesta task).

- [ ] **Step 5: Commit**

```bash
git add commission_tool/core/apuracao.py tests/test_apuracao.py docs/regras_calculo.md
git commit -m "feat: identifica modelos sem regra e CENs sem gerente; remove padrao de 1%"
```

---

### Task 4: Trava na tela de Apurações, popup, navegação e Reaplicar regras

**Files:**
- Modify: `commission_tool/ui/app.py`:
  - import (linha ~18): troca `apply_frontend_default_fat_commission` por `find_pending_registrations`
  - `refresh_machine_apuracao_state` (319)
  - `render_sidebar` (577: `st.radio`)
  - `render_machine_extraction` (2248-2388)
  - `render_settings_view` (3486)
  - `main` (3724-3727)

**Interfaces:**
- Consumes: `find_pending_registrations` (Task 3).
- Produces (session_state): `nav_view` (key do radio), `machine_pending_models_df`, `machine_pending_cens_df`, `machine_pending_dialog_shown: bool`.

- [ ] **Step 1: Navegação** — `st.radio(..., key="nav_view")`; função `go_to_settings() -> None` que faz `st.session_state.nav_view = "Configurações"` (usada como `on_click`, nunca chamada depois do radio renderizado).

- [ ] **Step 2: Reaplicar regras** — `refresh_machine_apuracao_state(reload_rules: bool = False) -> None`: com `True`, sempre relê `read_model_fat_rates`, `read_model_margin_rates`, `read_manager_relations` e grava `machine_rate_fat_df`, `machine_rate_margin_df`, `manager_relations_df`; também zera `machine_pending_dialog_shown`. Chamadas existentes continuam sem argumento.

- [ ] **Step 3: Remover o padrão de 1%** — apagar toggle, lógica de `machine_default_one_percent_*` (render e `main`), a chamada à função removida e o trecho "Padrão 1% aplicado" do caption.

- [ ] **Step 4: Trava** — logo após `filter_old_emissions`, chamar `find_pending_registrations(df_machine)`, guardar nos dois session_state. Se algum não estiver vazio:
  - `@st.dialog("Cadastros pendentes", width="large")` `show_pending_dialog(models, cens)`: título explicando que a apuração foi interrompida, as duas tabelas (só as não vazias, com contagem) e o botão "Ir para Configurações" (`on_click=go_to_settings`).
  - Abrir o dialog só se `machine_pending_dialog_shown` for falso; marcar como verdadeiro.
  - Bloco fixo: `st.error` com a contagem, as duas tabelas, e três botões: "Ver pendências" (reabre o dialog), "Ir para Configurações" (`on_click=go_to_settings`), "Reaplicar regras" (`refresh_machine_apuracao_state(reload_rules=True)` + `st.rerun()`).
  - `return` — nada da tabela, validação, exclusão ou pagamento é renderizado.
  - No clique de "Extrair máquinas", zerar `machine_pending_dialog_shown`.

- [ ] **Step 5: Quadro em Configurações** — no topo de `render_settings_view`, se houver pendências no session_state, `st.warning("Pendências da última apuração")` com as duas tabelas e a dica de usar "Reaplicar regras" em Apurações depois de salvar.

- [ ] **Step 6: Verificar** — `.venv/Scripts/python -c "import commission_tool.ui.app"` sem erro; suíte completa OK. Verificação manual com o usuário (precisa do SQL Server): extrair uma competência → popup com todas as pendências de uma vez; "Ir para Configurações" troca a tela; cadastrar; voltar e "Reaplicar regras" → tabela aparece; desligar o filtro de 01/12/2025 → se surgirem linhas sem regra, a trava volta; validar Contas a Receber → `CVD JD` aparece APTO.

- [ ] **Step 7: Commit**

```bash
git add commission_tool/ui/app.py
git commit -m "feat: trava a apuracao com popup de cadastros pendentes e reaplicar regras"
```
