# Parte B — Comissão de Consórcio Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Substituir a planilha manual de premiação de consórcio por uma apuração no sistema, alimentada pelo upload dos CSV do sistema de consórcio, com de-paras e regras editáveis e sem risco de pagar a mesma cota duas vezes.

**Architecture:** Fluxo separado do de máquinas, em módulos próprios: um leitor de CSV, um núcleo de regras puro (pandas, sem I/O), uma camada de acesso ao Postgres com cinco tabelas novas e uma tela Streamlit própria. O resultado entra nos relatórios e e-mails existentes como colunas dedicadas.

**Tech Stack:** Python 3.9+, pandas, openpyxl, SQLAlchemy + psycopg (Postgres), Streamlit, unittest.

**Spec:** `docs/superpowers/specs/2026-09-22-venda-direta-e-consorcio-design.md` (Parte B)

## Global Constraints

- Rodar os testes com `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`. Nenhum teste existente pode quebrar.
- Nenhum teste acessa Postgres de verdade: usar `unittest.mock.patch` sobre `get_engine`, `ensure_consorcio_tables` e `pd.read_sql`, como em `tests/test_postgres_mapping.py`.
- **Os CSV reais contêm nomes de consorciados e não entram no repositório.** Fixtures de teste usam nomes inventados.
- Percentuais vigentes em 08/2026, que vão como carga inicial da tabela de regras: parcela `001 / 003` → CEN 0,84; `002 / 003` → gestor 0,21 e coordenador 0,15; `003 / 003` → tudo zero.
- Coordenador inicial: `DJALMA RODRIGUES DE PAULA JUNIOR`, e-mail `djalma.junior@hohl.com.br`.
- Base de cálculo é sempre `VL_Bem`. O `VL_Comissao` do arquivo é informativo e nunca entra no cálculo.
- Chave única de pagamento: `cd_grupo` + `cd_cota` + `versao` + `parcela`.
- O sistema nunca preenche um de-para sozinho. Vendedor desconhecido é cadastrado com `cod_vendedor` **em branco** e a linha fica bloqueada.
- Um commit por task, mensagem em português, terminando com `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.

---

## File Structure

| Arquivo | Responsabilidade | Task |
|---|---|---|
| `commission_tool/io/consorcio_csv.py` | Ler e normalizar o CSV do sistema de consórcio | 1 |
| `commission_tool/data/sources/postgres_consorcio.py` | As cinco tabelas do consórcio: criação, leitura e gravação | 2, 5 |
| `commission_tool/core/consorcio.py` | De-paras, status, cálculo e tabelas de premiação (funções puras) | 3, 4 |
| `commission_tool/io/consorcio_excel.py` | Exportação do workbook `Base` + `Premiação` | 6 |
| `commission_tool/ui/consorcio.py` | Tela de apuração e seções de Configurações | 6, 7 |
| `commission_tool/core/reports.py` | Colunas Consórcio e Gestor Consórcio; quadro do coordenador | 8 |
| `commission_tool/core/report_email.py` | Aba de consórcio nos e-mails e e-mail do coordenador | 9 |
| `commission_tool/ui/app.py` | Item de menu e chamada das seções novas | 6, 7 |
| `scripts/conferir_consorcio_agosto.py` | Conferência local contra a planilha de agosto | 10 |

---

### Task 1: Leitor do CSV do consórcio

**Files:**
- Create: `commission_tool/io/consorcio_csv.py`
- Test: `tests/test_consorcio_csv.py`

**Interfaces:**
- Consumes: nada.
- Produces:
  - `ConsorcioFileError(ValueError)` com atributo `missing: list[str]`.
  - `read_consorcio_csv(uploaded_file, file_name: str = "") -> pd.DataFrame` com as colunas `arquivo_origem`, `cd_pedido`, `nm_produto`, `cd_pdv`, `nm_pdv`, `cd_vendedor_consorcio`, `nm_vendedor_consorcio`, `consorciado`, `cd_grupo`, `cd_cota`, `versao`, `st_contemplacao`, `cd_periodo`, `dt_alocacao`, `dt_aprovacao`, `vl_bem` (float), `parcela`, `pe_comissao` (float), `vl_comissao` (float). Códigos ficam como texto, preservando zeros à esquerda.

- [ ] **Step 1: Escrever os testes**

Criar `tests/test_consorcio_csv.py`:

```python
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
    prefixo = "\ufeff" if bom else ""
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_csv -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'commission_tool.io.consorcio_csv'`

- [ ] **Step 3: Implementar o leitor**

Criar `commission_tool/io/consorcio_csv.py`:

```python
"""Reader for the consortium system CSV reports."""

from __future__ import annotations

import pandas as pd

from commission_tool.core.formatting import parse_br_number


TEXT_COLUMNS = {
    "CD_Pedido": "cd_pedido",
    "NM_Produto": "nm_produto",
    "CD_Comissionado_PDV": "cd_pdv",
    "NM_Comissionado_PDV": "nm_pdv",
    "Consorciado": "consorciado",
    "CD_Grupo": "cd_grupo",
    "CD_Cota": "cd_cota",
    "Versao": "versao",
    "ST_Contemplacao": "st_contemplacao",
    "CD_Periodo": "cd_periodo",
    "DT_Alocacao": "dt_alocacao",
    "DT_Aprovacao": "dt_aprovacao",
    "Parcela": "parcela",
}
NUMERIC_COLUMNS = {
    "VL_Bem": "vl_bem",
    "PE_Comissao": "pe_comissao",
    "VL_Comissao": "vl_comissao",
}
VENDEDOR_CODE_COLUMNS = [
    "CD_Comissionado_Vendedor_Atual",
    "CD_Comissionado_Vendedor_Original",
]
VENDEDOR_NAME_COLUMNS = [
    "NM_Comissionado_Vendedor_Atual",
    "NM_Comissionado_Vendedor_Original",
]


class ConsorcioFileError(ValueError):
    """Raised when the uploaded file does not match the expected layout."""

    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__("Colunas obrigatórias ausentes: " + ", ".join(missing))


def _first_present(columns: list[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def read_consorcio_csv(uploaded_file, file_name: str = "") -> pd.DataFrame:
    """Read one consortium CSV into a normalized DataFrame."""
    raw = pd.read_csv(
        uploaded_file,
        sep=";",
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
    )
    raw.columns = [str(column).strip() for column in raw.columns]
    columns = list(raw.columns)

    missing = [column for column in {**TEXT_COLUMNS, **NUMERIC_COLUMNS} if column not in columns]
    code_column = _first_present(columns, VENDEDOR_CODE_COLUMNS)
    name_column = _first_present(columns, VENDEDOR_NAME_COLUMNS)
    if code_column is None:
        missing.append(VENDEDOR_CODE_COLUMNS[0])
    if name_column is None:
        missing.append(VENDEDOR_NAME_COLUMNS[0])
    if missing:
        raise ConsorcioFileError(sorted(missing))

    data = {target: raw[source].astype(str).str.strip() for source, target in TEXT_COLUMNS.items()}
    data["cd_vendedor_consorcio"] = raw[code_column].astype(str).str.strip()
    data["nm_vendedor_consorcio"] = raw[name_column].astype(str).str.strip()
    for source, target in NUMERIC_COLUMNS.items():
        data[target] = raw[source].apply(parse_br_number)

    prepared = pd.DataFrame(data)
    prepared.insert(0, "arquivo_origem", str(file_name or ""))
    return prepared.reset_index(drop=True)
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_csv -v`
Expected: PASS nos 5 testes.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/io/consorcio_csv.py tests/test_consorcio_csv.py
git commit -m "feat: leitor dos CSV do sistema de consorcio

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Tabelas de configuração do consórcio no Postgres

**Files:**
- Create: `commission_tool/data/sources/postgres_consorcio.py`
- Test: `tests/test_postgres_consorcio.py`

**Interfaces:**
- Consumes: `get_engine` de `commission_tool.data.sources.postgres`.
- Produces:
  - Constantes `CONSORCIO_PDV_TABLE = "comissao_consorcio_pdv"`, `CONSORCIO_VENDEDOR_TABLE = "comissao_consorcio_vendedor"`, `CONSORCIO_REGRAS_TABLE = "comissao_consorcio_regras"`, `CONSORCIO_COORDENADOR_TABLE = "comissao_consorcio_coordenador"`, `CONSORCIO_PAID_TABLE = "comissoes_consorcio_pagas"`.
  - `ensure_consorcio_tables() -> None`
  - `read_consorcio_pdv_map() -> pd.DataFrame` (`cd_pdv`, `nm_pdv`, `filial`)
  - `replace_active_consorcio_pdv_map(df) -> int`
  - `read_consorcio_vendedor_map() -> pd.DataFrame` (`cd_vendedor_consorcio`, `nm_vendedor_consorcio`, `cod_vendedor`)
  - `replace_active_consorcio_vendedor_map(df) -> int`
  - `register_missing_consorcio_vendedores(df) -> int` (insere pendentes com `cod_vendedor` vazio)
  - `read_consorcio_regras() -> pd.DataFrame` (`parcela`, `percentual_cen`, `percentual_gestor`, `percentual_coordenador`)
  - `replace_active_consorcio_regras(df) -> int`
  - `read_consorcio_coordenador() -> dict` (`{"nome": str, "email": str}`)
  - `save_consorcio_coordenador(nome: str, email: str) -> None`

- [ ] **Step 1: Escrever os testes**

Criar `tests/test_postgres_consorcio.py`:

```python
import unittest
from unittest.mock import patch

import pandas as pd

from commission_tool.data.sources.postgres_consorcio import (
    CONSORCIO_PDV_TABLE,
    CONSORCIO_VENDEDOR_TABLE,
    _prepare_pdv_map,
    _prepare_vendedor_map,
    register_missing_consorcio_vendedores,
)


class FakeConnection:
    def __init__(self):
        self.executed_sql = []

    def execute(self, statement, *args, **kwargs):
        self.executed_sql.append(str(statement))


class FakeEngine:
    def __init__(self, conn):
        self.conn = conn

    def begin(self):
        return self

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, traceback):
        return False


class PrepareMapTests(unittest.TestCase):
    def test_pdv_map_normalizes_code_without_leading_zeros(self):
        df = pd.DataFrame(
            [
                {"CD_Comissionado_PDV": "002001", "NM_Comissionado_PDV": "HOHL GOIANIA", "Filial": " Loja Goiania "},
                {"CD_Comissionado_PDV": "", "NM_Comissionado_PDV": "SEM CODIGO", "Filial": "Loja X"},
            ]
        )

        prepared = _prepare_pdv_map(df)

        self.assertEqual(prepared["cd_pdv"].tolist(), ["2001"])
        self.assertEqual(prepared["filial"].tolist(), ["Loja Goiania"])
        self.assertTrue(prepared["ativo"].all())

    def test_vendedor_map_keeps_blank_cod_vendedor(self):
        df = pd.DataFrame(
            [
                {"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A", "cod_vendedor": " 00188 "},
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B", "cod_vendedor": None},
            ]
        )

        prepared = _prepare_vendedor_map(df)

        self.assertEqual(prepared["cd_vendedor_consorcio"].tolist(), ["002197", "002697"])
        self.assertEqual(prepared["cod_vendedor"].tolist(), ["00188", ""])


class RegisterMissingTests(unittest.TestCase):
    def test_registers_only_unknown_sellers_with_blank_cod_vendedor(self):
        conn = FakeConnection()
        rows = pd.DataFrame(
            [
                {"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A"},
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B"},
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B"},
            ]
        )
        known = pd.DataFrame(
            [{"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A", "cod_vendedor": "00188"}]
        )

        captured = {}

        def fake_to_sql(self, name, con, **kwargs):
            captured["name"] = name
            captured["df"] = self.copy()

        with patch("commission_tool.data.sources.postgres_consorcio.ensure_consorcio_tables"), patch(
            "commission_tool.data.sources.postgres_consorcio.get_engine",
            return_value=FakeEngine(conn),
        ), patch(
            "commission_tool.data.sources.postgres_consorcio.read_consorcio_vendedor_map",
            return_value=known,
        ), patch.object(pd.DataFrame, "to_sql", fake_to_sql):
            inserted = register_missing_consorcio_vendedores(rows)

        self.assertEqual(inserted, 1)
        self.assertEqual(captured["name"], CONSORCIO_VENDEDOR_TABLE)
        self.assertEqual(captured["df"]["cd_vendedor_consorcio"].tolist(), ["002697"])
        self.assertEqual(captured["df"]["cod_vendedor"].tolist(), [""])
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_postgres_consorcio -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'commission_tool.data.sources.postgres_consorcio'`

- [ ] **Step 3: Criar o módulo com as tabelas e as cargas iniciais**

Criar `commission_tool/data/sources/postgres_consorcio.py`:

```python
"""Postgres access for the consortium commission flow."""

from __future__ import annotations

import pandas as pd

from commission_tool.core.formatting import parse_br_number, parse_percent_points
from commission_tool.data.sources.postgres import get_engine


CONSORCIO_PDV_TABLE = "comissao_consorcio_pdv"
CONSORCIO_VENDEDOR_TABLE = "comissao_consorcio_vendedor"
CONSORCIO_REGRAS_TABLE = "comissao_consorcio_regras"
CONSORCIO_COORDENADOR_TABLE = "comissao_consorcio_coordenador"
CONSORCIO_PAID_TABLE = "comissoes_consorcio_pagas"

DEFAULT_REGRAS = [
    {"parcela": "001 / 003", "percentual_cen": 0.84, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
    {"parcela": "002 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.21, "percentual_coordenador": 0.15},
    {"parcela": "003 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
]
DEFAULT_COORDENADOR = {"nome": "DJALMA RODRIGUES DE PAULA JUNIOR", "email": "djalma.junior@hohl.com.br"}


def normalize_pdv(value) -> str:
    """Normalize a PDV code: '002001' and '2001' are the same PDV."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    if text.isdigit():
        return text.lstrip("0") or "0"
    return text.upper()


def ensure_consorcio_tables() -> None:
    """Create the consortium tables when they do not exist and seed the rules."""
    from sqlalchemy import text

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_PDV_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    cd_pdv TEXT NOT NULL,
                    nm_pdv TEXT,
                    filial TEXT NOT NULL,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_VENDEDOR_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    cd_vendedor_consorcio TEXT NOT NULL,
                    nm_vendedor_consorcio TEXT,
                    cod_vendedor TEXT NOT NULL DEFAULT '',
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_REGRAS_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    parcela TEXT NOT NULL,
                    percentual_cen NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    percentual_gestor NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    percentual_coordenador NUMERIC(12, 6) NOT NULL DEFAULT 0,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_COORDENADOR_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    nome TEXT NOT NULL,
                    email TEXT,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {CONSORCIO_PAID_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    paid_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    competencia_ano INTEGER NOT NULL,
                    competencia_mes INTEGER NOT NULL,
                    mes_ano_comissao TEXT NOT NULL,
                    arquivo_origem TEXT,
                    cd_pedido TEXT,
                    cd_grupo TEXT NOT NULL,
                    cd_cota TEXT NOT NULL,
                    versao TEXT NOT NULL,
                    parcela TEXT NOT NULL,
                    st_contemplacao TEXT,
                    dt_alocacao DATE,
                    dt_aprovacao DATE,
                    consorciado TEXT,
                    vl_bem NUMERIC(18, 2),
                    cd_pdv TEXT,
                    filial TEXT,
                    cd_vendedor_consorcio TEXT,
                    nm_vendedor_consorcio TEXT,
                    cod_vendedor TEXT,
                    cen TEXT,
                    gerente TEXT,
                    coordenador TEXT,
                    percentual_cen NUMERIC(12, 6),
                    valor_comissao_cen NUMERIC(18, 2),
                    percentual_gestor NUMERIC(12, 6),
                    valor_comissao_gestor NUMERIC(18, 2),
                    percentual_coordenador NUMERIC(12, 6),
                    valor_comissao_coordenador NUMERIC(18, 2)
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {CONSORCIO_PAID_TABLE}_cota_parcela_uidx
                ON {CONSORCIO_PAID_TABLE} (cd_grupo, cd_cota, versao, parcela)
                """
            )
        )
        seeded = conn.execute(text(f"SELECT COUNT(*) FROM {CONSORCIO_REGRAS_TABLE}")).scalar()
        if not seeded:
            pd.DataFrame(DEFAULT_REGRAS).assign(ativo=True).to_sql(
                CONSORCIO_REGRAS_TABLE, conn, if_exists="append", index=False
            )
        seeded_coord = conn.execute(text(f"SELECT COUNT(*) FROM {CONSORCIO_COORDENADOR_TABLE}")).scalar()
        if not seeded_coord:
            pd.DataFrame([DEFAULT_COORDENADOR]).assign(ativo=True).to_sql(
                CONSORCIO_COORDENADOR_TABLE, conn, if_exists="append", index=False
            )
```

- [ ] **Step 4: Implementar preparo, leitura e gravação**

No mesmo arquivo, acrescentar:

```python
def _prepare_pdv_map(df: pd.DataFrame) -> pd.DataFrame:
    source = df.rename(
        columns={
            "CD_Comissionado_PDV": "cd_pdv",
            "NM_Comissionado_PDV": "nm_pdv",
            "Filial": "filial",
            "PDV": "cd_pdv",
        }
    )
    prepared = pd.DataFrame(
        {
            "cd_pdv": source.get("cd_pdv", pd.Series(dtype=object)).apply(normalize_pdv),
            "nm_pdv": source.get("nm_pdv", pd.Series(dtype=object)).fillna("").astype(str).str.strip(),
            "filial": source.get("filial", pd.Series(dtype=object)).fillna("").astype(str).str.strip(),
        }
    )
    prepared = prepared[prepared["cd_pdv"].ne("") & prepared["filial"].ne("")]
    prepared = prepared.drop_duplicates(subset=["cd_pdv"], keep="first")
    prepared["ativo"] = True
    return prepared.reset_index(drop=True)


def _prepare_vendedor_map(df: pd.DataFrame) -> pd.DataFrame:
    source = df.rename(
        columns={
            "CD_Comissionado_Vendedor": "cd_vendedor_consorcio",
            "NM_Comissionado_Vendedor": "nm_vendedor_consorcio",
            "Cod Vendedor": "cod_vendedor",
        }
    )
    prepared = pd.DataFrame(
        {
            "cd_vendedor_consorcio": source.get("cd_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
            "nm_vendedor_consorcio": source.get("nm_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
            "cod_vendedor": source.get("cod_vendedor", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper(),
        }
    )
    prepared = prepared[prepared["cd_vendedor_consorcio"].ne("")]
    prepared = prepared.drop_duplicates(subset=["cd_vendedor_consorcio"], keep="first")
    prepared["ativo"] = True
    return prepared.reset_index(drop=True)


def _read_active(table: str, columns: list[str]) -> pd.DataFrame:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    query = text(f"SELECT {', '.join(columns)} FROM {table} WHERE ativo IS TRUE ORDER BY id")
    return pd.read_sql(query, engine)


def _replace_active(table: str, prepared: pd.DataFrame) -> int:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE {table} SET ativo = FALSE WHERE ativo IS TRUE"))
        prepared.to_sql(table, conn, if_exists="append", index=False)
    return len(prepared)


def read_consorcio_pdv_map() -> pd.DataFrame:
    return _read_active(CONSORCIO_PDV_TABLE, ["cd_pdv", "nm_pdv", "filial"])


def replace_active_consorcio_pdv_map(df: pd.DataFrame) -> int:
    return _replace_active(CONSORCIO_PDV_TABLE, _prepare_pdv_map(df))


def read_consorcio_vendedor_map() -> pd.DataFrame:
    return _read_active(
        CONSORCIO_VENDEDOR_TABLE,
        ["cd_vendedor_consorcio", "nm_vendedor_consorcio", "cod_vendedor"],
    )


def replace_active_consorcio_vendedor_map(df: pd.DataFrame) -> int:
    return _replace_active(CONSORCIO_VENDEDOR_TABLE, _prepare_vendedor_map(df))


def register_missing_consorcio_vendedores(df: pd.DataFrame) -> int:
    """Insert unknown consortium sellers with a blank cod_vendedor, for later fill-in."""
    ensure_consorcio_tables()
    known = read_consorcio_vendedor_map()
    known_codes = set(known.get("cd_vendedor_consorcio", pd.Series(dtype=object)).astype(str).str.strip())

    candidates = pd.DataFrame(
        {
            "cd_vendedor_consorcio": df.get("cd_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
            "nm_vendedor_consorcio": df.get("nm_vendedor_consorcio", pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip(),
        }
    )
    candidates = candidates[candidates["cd_vendedor_consorcio"].ne("")]
    candidates = candidates[~candidates["cd_vendedor_consorcio"].isin(known_codes)]
    candidates = candidates.drop_duplicates(subset=["cd_vendedor_consorcio"], keep="first")
    if candidates.empty:
        return 0

    candidates["cod_vendedor"] = ""
    candidates["ativo"] = True
    engine = get_engine()
    with engine.begin() as conn:
        candidates.to_sql(CONSORCIO_VENDEDOR_TABLE, conn, if_exists="append", index=False)
    return len(candidates)


def read_consorcio_regras() -> pd.DataFrame:
    return _read_active(
        CONSORCIO_REGRAS_TABLE,
        ["parcela", "percentual_cen", "percentual_gestor", "percentual_coordenador"],
    )


def replace_active_consorcio_regras(df: pd.DataFrame) -> int:
    prepared = pd.DataFrame(
        {
            "parcela": df.get("parcela", pd.Series(dtype=object)).fillna("").astype(str).str.strip(),
            "percentual_cen": df.get("percentual_cen", pd.Series(dtype=object)).apply(parse_percent_points),
            "percentual_gestor": df.get("percentual_gestor", pd.Series(dtype=object)).apply(parse_percent_points),
            "percentual_coordenador": df.get("percentual_coordenador", pd.Series(dtype=object)).apply(
                parse_percent_points
            ),
        }
    )
    prepared = prepared[prepared["parcela"].ne("")].drop_duplicates(subset=["parcela"], keep="first")
    prepared["ativo"] = True
    return _replace_active(CONSORCIO_REGRAS_TABLE, prepared.reset_index(drop=True))


def read_consorcio_coordenador() -> dict:
    rows = _read_active(CONSORCIO_COORDENADOR_TABLE, ["nome", "email"])
    if rows.empty:
        return dict(DEFAULT_COORDENADOR)
    return {"nome": str(rows.loc[0, "nome"] or ""), "email": str(rows.loc[0, "email"] or "")}


def save_consorcio_coordenador(nome: str, email: str) -> None:
    prepared = pd.DataFrame([{"nome": str(nome).strip(), "email": str(email).strip(), "ativo": True}])
    _replace_active(CONSORCIO_COORDENADOR_TABLE, prepared)
```

Nota: `parse_br_number` fica importado para uso na Task 5 (gravação dos pagos); se o linter reclamar antes disso, importar apenas na Task 5.

- [ ] **Step 5: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_postgres_consorcio -v`
Expected: PASS nos 3 testes.

- [ ] **Step 6: Commit**

```bash
git add commission_tool/data/sources/postgres_consorcio.py tests/test_postgres_consorcio.py
git commit -m "feat: tabelas de configuracao do consorcio no Postgres

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Núcleo de regras — de-paras, status e cálculo

**Files:**
- Create: `commission_tool/core/consorcio.py`
- Test: `tests/test_consorcio_core.py`

**Interfaces:**
- Consumes: DataFrame de `read_consorcio_csv` (Task 1) e os mapas de `postgres_consorcio` (Task 2).
- Produces:
  - Constantes `STATUS_APTO = "✅ APTO"`, `STATUS_SEM_COMISSAO = "Sem comissão"`, `STATUS_BLOQUEADO_PDV`, `STATUS_BLOQUEADO_VENDEDOR`, `STATUS_BLOQUEADO_GERENTE`, `STATUS_BLOQUEADO_JA_PAGO`, `STATUS_BLOQUEADO_DUPLICADO`, `STATUS_BLOQUEADO_REGRA`, `STATUS_BLOQUEADO_VALOR`.
  - `cota_key(row_or_df) -> str | pd.Series`
  - `enrich_consorcio_rows(df, pdv_map, vendedor_map, manager_relations, regras, coordenador, paid_keys=None) -> pd.DataFrame` com as colunas de entrada mais `filial`, `cod_vendedor`, `cen`, `gerente`, `coordenador`, `percentual_cen`, `valor_comissao_cen`, `percentual_gestor`, `valor_comissao_gestor`, `percentual_coordenador`, `valor_comissao_coordenador`, `status` e `apto` (bool).

- [ ] **Step 1: Escrever os testes**

Criar `tests/test_consorcio_core.py`:

```python
import unittest

import pandas as pd

from commission_tool.core.consorcio import (
    STATUS_APTO,
    STATUS_BLOQUEADO_DUPLICADO,
    STATUS_BLOQUEADO_GERENTE,
    STATUS_BLOQUEADO_JA_PAGO,
    STATUS_BLOQUEADO_PDV,
    STATUS_BLOQUEADO_REGRA,
    STATUS_BLOQUEADO_VALOR,
    STATUS_BLOQUEADO_VENDEDOR,
    STATUS_SEM_COMISSAO,
    build_premiacao_tables,
    enrich_consorcio_rows,
)


PDV_MAP = pd.DataFrame([{"cd_pdv": "208016", "nm_pdv": "HOHL ACREUNA", "filial": "Loja Acreuna"}])
VENDEDOR_MAP = pd.DataFrame(
    [
        {"cd_vendedor_consorcio": "002197", "nm_vendedor_consorcio": "VENDEDOR A", "cod_vendedor": "00188"},
        {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "VENDEDOR B", "cod_vendedor": ""},
        {"cd_vendedor_consorcio": "002356", "nm_vendedor_consorcio": "VENDEDOR C", "cod_vendedor": "A69853"},
    ]
)
RELATIONS = pd.DataFrame(
    [{"cod_vendedor": "00188", "vendedor": "VENDEDOR A", "gerente": "Reginaldo Pereira Garcia"}]
)
REGRAS = pd.DataFrame(
    [
        {"parcela": "001 / 003", "percentual_cen": 0.84, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
        {"parcela": "002 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.21, "percentual_coordenador": 0.15},
        {"parcela": "003 / 003", "percentual_cen": 0.0, "percentual_gestor": 0.0, "percentual_coordenador": 0.0},
    ]
)
COORDENADOR = {"nome": "DJALMA RODRIGUES DE PAULA JUNIOR", "email": "djalma.junior@hohl.com.br"}


def _row(**overrides) -> dict:
    row = {
        "arquivo_origem": "Acreuna.csv",
        "cd_pedido": "4504255710",
        "cd_pdv": "208016",
        "nm_pdv": "HOHL ACREUNA",
        "cd_vendedor_consorcio": "002197",
        "nm_vendedor_consorcio": "VENDEDOR A",
        "consorciado": "CONSORCIADO TESTE",
        "cd_grupo": "002362",
        "cd_cota": "0025",
        "versao": "00",
        "st_contemplacao": "Não Contemplada",
        "parcela": "001 / 003",
        "vl_bem": 200000.0,
    }
    row.update(overrides)
    return row


def _enrich(rows, paid_keys=None):
    return enrich_consorcio_rows(
        pd.DataFrame(rows),
        pdv_map=PDV_MAP,
        vendedor_map=VENDEDOR_MAP,
        manager_relations=RELATIONS,
        regras=REGRAS,
        coordenador=COORDENADOR,
        paid_keys=paid_keys,
    )


class EnrichTests(unittest.TestCase):
    def test_first_installment_pays_cen_only(self):
        result = _enrich([_row()])

        self.assertEqual(result.loc[0, "filial"], "Loja Acreuna")
        self.assertEqual(result.loc[0, "cod_vendedor"], "00188")
        self.assertEqual(result.loc[0, "cen"], "VENDEDOR A")
        self.assertEqual(result.loc[0, "gerente"], "Reginaldo Pereira Garcia")
        self.assertAlmostEqual(result.loc[0, "valor_comissao_cen"], 1680.0, places=2)
        self.assertEqual(result.loc[0, "valor_comissao_gestor"], 0.0)
        self.assertEqual(result.loc[0, "valor_comissao_coordenador"], 0.0)
        self.assertEqual(result.loc[0, "status"], STATUS_APTO)
        self.assertTrue(result.loc[0, "apto"])

    def test_second_installment_pays_manager_and_coordinator(self):
        result = _enrich([_row(parcela="002 / 003", vl_bem=206994.15)])

        self.assertEqual(result.loc[0, "valor_comissao_cen"], 0.0)
        self.assertAlmostEqual(result.loc[0, "valor_comissao_gestor"], 434.687715, places=6)
        self.assertAlmostEqual(result.loc[0, "valor_comissao_coordenador"], 310.491225, places=6)
        self.assertEqual(result.loc[0, "coordenador"], "DJALMA RODRIGUES DE PAULA JUNIOR")
        self.assertEqual(result.loc[0, "status"], STATUS_APTO)

    def test_third_installment_has_no_commission(self):
        result = _enrich([_row(parcela="003 / 003")])

        self.assertEqual(result.loc[0, "status"], STATUS_SEM_COMISSAO)
        self.assertFalse(result.loc[0, "apto"])
        self.assertEqual(result.loc[0, "valor_comissao_cen"], 0.0)

    def test_blocks_unknown_pdv(self):
        result = _enrich([_row(cd_pdv="002001")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_PDV)
        self.assertFalse(result.loc[0, "apto"])

    def test_pdv_matches_ignoring_leading_zeros(self):
        result = _enrich([_row(cd_pdv="0208016")])

        self.assertEqual(result.loc[0, "filial"], "Loja Acreuna")

    def test_blocks_seller_without_de_para(self):
        result = _enrich([_row(cd_vendedor_consorcio="002697", nm_vendedor_consorcio="VENDEDOR B")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_VENDEDOR)

    def test_blocks_unknown_seller_not_in_map(self):
        result = _enrich([_row(cd_vendedor_consorcio="009999", nm_vendedor_consorcio="VENDEDOR NOVO")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_VENDEDOR)

    def test_blocks_second_installment_without_manager(self):
        result = _enrich(
            [_row(cd_vendedor_consorcio="002356", nm_vendedor_consorcio="VENDEDOR C", parcela="002 / 003")]
        )

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_GERENTE)

    def test_first_installment_does_not_require_manager(self):
        result = _enrich([_row(cd_vendedor_consorcio="002356", nm_vendedor_consorcio="VENDEDOR C")])

        self.assertEqual(result.loc[0, "status"], STATUS_APTO)

    def test_blocks_already_paid_cota(self):
        result = _enrich([_row()], paid_keys={"002362||0025||00||001 / 003": "08/2026"})

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_JA_PAGO.format(competencia="08/2026"))

    def test_blocks_duplicate_inside_upload(self):
        result = _enrich([_row(arquivo_origem="A.csv"), _row(arquivo_origem="B.csv")])

        self.assertEqual(result.loc[0, "status"], STATUS_APTO)
        self.assertEqual(result.loc[1, "status"], STATUS_BLOQUEADO_DUPLICADO)

    def test_blocks_parcela_without_rule(self):
        result = _enrich([_row(parcela="001 / 004")])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_REGRA)

    def test_blocks_invalid_vl_bem(self):
        result = _enrich([_row(vl_bem=0.0), _row(cd_cota="0026", vl_bem=None)])

        self.assertEqual(result.loc[0, "status"], STATUS_BLOQUEADO_VALOR)
        self.assertEqual(result.loc[1, "status"], STATUS_BLOQUEADO_VALOR)


class PremiacaoTests(unittest.TestCase):
    def test_builds_three_summary_tables(self):
        rows = _enrich(
            [
                _row(),
                _row(cd_cota="0026"),
                _row(cd_cota="0027", parcela="002 / 003", vl_bem=206994.15),
            ]
        )

        tables = build_premiacao_tables(rows)

        self.assertAlmostEqual(tables["cen"].loc[0, "Valor a pagar"], 3360.0, places=2)
        self.assertEqual(tables["cen"].loc[0, "CEN"], "VENDEDOR A")
        self.assertAlmostEqual(tables["gestor"].loc[0, "Valor a pagar"], 434.687715, places=6)
        self.assertEqual(tables["gestor"].loc[0, "Gestor"], "Reginaldo Pereira Garcia")
        self.assertAlmostEqual(tables["coordenador"].loc[0, "Valor a pagar"], 310.491225, places=6)
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_core -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'commission_tool.core.consorcio'`

- [ ] **Step 3: Implementar o núcleo**

Criar `commission_tool/core/consorcio.py`:

```python
"""Business rules for the consortium commission flow."""

from __future__ import annotations

import unicodedata

import pandas as pd

from commission_tool.data.sources.postgres_consorcio import normalize_pdv


STATUS_APTO = "✅ APTO"
STATUS_SEM_COMISSAO = "Sem comissão"
STATUS_BLOQUEADO_PDV = "❌ Bloqueado: PDV sem de-para"
STATUS_BLOQUEADO_VENDEDOR = "❌ Bloqueado: vendedor sem de-para"
STATUS_BLOQUEADO_GERENTE = "❌ Bloqueado: sem gerente"
STATUS_BLOQUEADO_JA_PAGO = "❌ Bloqueado: já pago em {competencia}"
STATUS_BLOQUEADO_DUPLICADO = "❌ Bloqueado: duplicado no upload"
STATUS_BLOQUEADO_REGRA = "❌ Bloqueado: parcela sem regra"
STATUS_BLOQUEADO_VALOR = "❌ Bloqueado: valor do bem inválido"


def normalize_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = unicodedata.normalize("NFKD", str(value).strip())
    return "".join(char for char in text if not unicodedata.combining(char)).upper()


def normalize_code(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().upper()


def normalize_parcela(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return " ".join(str(value).split()).upper()


def cota_key(df: pd.DataFrame) -> pd.Series:
    """Unique key of a cota installment: grupo || cota || versao || parcela."""
    return (
        df["cd_grupo"].apply(normalize_code)
        + "||"
        + df["cd_cota"].apply(normalize_code)
        + "||"
        + df["versao"].apply(normalize_code)
        + "||"
        + df["parcela"].apply(normalize_parcela)
    )


def enrich_consorcio_rows(
    df: pd.DataFrame,
    pdv_map: pd.DataFrame | None,
    vendedor_map: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
    regras: pd.DataFrame | None,
    coordenador: dict | None,
    paid_keys: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Resolve de-paras, apply the rules and classify every row with a status."""
    result = df.copy().reset_index(drop=True)
    if result.empty:
        for column in [
            "filial",
            "cod_vendedor",
            "cen",
            "gerente",
            "coordenador",
            "percentual_cen",
            "valor_comissao_cen",
            "percentual_gestor",
            "valor_comissao_gestor",
            "percentual_coordenador",
            "valor_comissao_coordenador",
            "status",
        ]:
            result[column] = pd.Series(dtype=object)
        result["apto"] = pd.Series(dtype=bool)
        return result

    coordenador = coordenador or {}
    result["__pdv_key"] = result["cd_pdv"].apply(normalize_pdv)
    result["__vendedor_key"] = result["cd_vendedor_consorcio"].apply(normalize_code)
    result["__parcela"] = result["parcela"].apply(normalize_parcela)
    result["__cota_key"] = cota_key(result)

    pdv_lookup = {}
    if pdv_map is not None and not pdv_map.empty:
        pdv_lookup = dict(
            zip(pdv_map["cd_pdv"].apply(normalize_pdv), pdv_map["filial"].fillna("").astype(str).str.strip())
        )
    result["filial"] = result["__pdv_key"].map(pdv_lookup).fillna("")

    vendedor_lookup = {}
    if vendedor_map is not None and not vendedor_map.empty:
        vendedor_lookup = dict(
            zip(
                vendedor_map["cd_vendedor_consorcio"].apply(normalize_code),
                vendedor_map["cod_vendedor"].fillna("").astype(str).str.strip().str.upper(),
            )
        )
    result["cod_vendedor"] = result["__vendedor_key"].map(vendedor_lookup).fillna("")

    cen_lookup: dict[str, str] = {}
    gerente_lookup: dict[str, str] = {}
    if manager_relations is not None and not manager_relations.empty:
        relations = manager_relations.copy()
        relations["__cod"] = relations["cod_vendedor"].apply(normalize_code)
        relations = relations[relations["__cod"].ne("")].drop_duplicates(subset=["__cod"], keep="first")
        cen_lookup = dict(zip(relations["__cod"], relations["vendedor"].fillna("").astype(str).str.strip()))
        gerente_lookup = dict(zip(relations["__cod"], relations["gerente"].fillna("").astype(str).str.strip()))
    result["cen"] = result["cod_vendedor"].map(cen_lookup).fillna("")
    result["cen"] = result["cen"].mask(result["cen"].eq(""), result["nm_vendedor_consorcio"])
    result["gerente"] = result["cod_vendedor"].map(gerente_lookup).fillna("")
    result["coordenador"] = str(coordenador.get("nome", "") or "")

    regras_lookup = {}
    if regras is not None and not regras.empty:
        prepared_regras = regras.copy()
        prepared_regras["__parcela"] = prepared_regras["parcela"].apply(normalize_parcela)
        prepared_regras = prepared_regras.drop_duplicates(subset=["__parcela"], keep="first")
        for _, regra in prepared_regras.iterrows():
            regras_lookup[regra["__parcela"]] = (
                float(regra["percentual_cen"] or 0),
                float(regra["percentual_gestor"] or 0),
                float(regra["percentual_coordenador"] or 0),
            )

    result["__tem_regra"] = result["__parcela"].isin(regras_lookup)
    result["percentual_cen"] = result["__parcela"].map(lambda p: regras_lookup.get(p, (0.0, 0.0, 0.0))[0])
    result["percentual_gestor"] = result["__parcela"].map(lambda p: regras_lookup.get(p, (0.0, 0.0, 0.0))[1])
    result["percentual_coordenador"] = result["__parcela"].map(
        lambda p: regras_lookup.get(p, (0.0, 0.0, 0.0))[2]
    )

    vl_bem = pd.to_numeric(result["vl_bem"], errors="coerce")
    result["__valor_invalido"] = vl_bem.isna() | vl_bem.le(0)
    base = vl_bem.fillna(0.0)
    result["valor_comissao_cen"] = base * result["percentual_cen"] / 100
    result["valor_comissao_gestor"] = base * result["percentual_gestor"] / 100
    result["valor_comissao_coordenador"] = base * result["percentual_coordenador"] / 100

    result["__paga_alguem"] = (
        result["percentual_cen"].gt(0)
        | result["percentual_gestor"].gt(0)
        | result["percentual_coordenador"].gt(0)
    )
    result["__paga_gestor"] = result["percentual_gestor"].gt(0) | result["percentual_coordenador"].gt(0)
    result["__duplicado"] = result.duplicated(subset=["__cota_key"], keep="first")
    paid_keys = paid_keys or {}
    result["__ja_pago"] = result["__cota_key"].map(paid_keys)

    def _status(row: pd.Series) -> str:
        if row["__valor_invalido"]:
            return STATUS_BLOQUEADO_VALOR
        if not row["__tem_regra"]:
            return STATUS_BLOQUEADO_REGRA
        if isinstance(row["__ja_pago"], str) and row["__ja_pago"]:
            return STATUS_BLOQUEADO_JA_PAGO.format(competencia=row["__ja_pago"])
        if row["__duplicado"]:
            return STATUS_BLOQUEADO_DUPLICADO
        if not row["filial"]:
            return STATUS_BLOQUEADO_PDV
        if not row["cod_vendedor"]:
            return STATUS_BLOQUEADO_VENDEDOR
        if row["__paga_gestor"] and not row["gerente"]:
            return STATUS_BLOQUEADO_GERENTE
        if not row["__paga_alguem"]:
            return STATUS_SEM_COMISSAO
        return STATUS_APTO

    result["status"] = result.apply(_status, axis=1)
    result["apto"] = result["status"].eq(STATUS_APTO)
    zerar = ~result["apto"]
    for column in ["valor_comissao_cen", "valor_comissao_gestor", "valor_comissao_coordenador"]:
        result.loc[zerar, column] = 0.0

    return result.drop(
        columns=[
            "__pdv_key",
            "__vendedor_key",
            "__parcela",
            "__tem_regra",
            "__valor_invalido",
            "__paga_alguem",
            "__paga_gestor",
            "__duplicado",
            "__ja_pago",
        ]
    )


def build_premiacao_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the three payout summaries: CEN, store manager and coordinator."""
    empty = {
        "cen": pd.DataFrame(columns=["CEN", "Filial", "Valor a pagar"]),
        "gestor": pd.DataFrame(columns=["Gestor", "Valor a pagar"]),
        "coordenador": pd.DataFrame(columns=["Coordenador", "Valor a pagar"]),
    }
    if df is None or df.empty:
        return empty

    paid = df[df.get("apto", pd.Series([False] * len(df), index=df.index))].copy()
    if paid.empty:
        return empty

    cen = (
        paid.groupby(["cen", "filial"], dropna=False)["valor_comissao_cen"]
        .sum()
        .reset_index()
        .rename(columns={"cen": "CEN", "filial": "Filial", "valor_comissao_cen": "Valor a pagar"})
        .sort_values("CEN")
        .reset_index(drop=True)
    )
    gestor = (
        paid[paid["gerente"].ne("")]
        .groupby("gerente", dropna=False)["valor_comissao_gestor"]
        .sum()
        .reset_index()
        .rename(columns={"gerente": "Gestor", "valor_comissao_gestor": "Valor a pagar"})
        .sort_values("Gestor")
        .reset_index(drop=True)
    )
    coordenador = (
        paid.groupby("coordenador", dropna=False)["valor_comissao_coordenador"]
        .sum()
        .reset_index()
        .rename(columns={"coordenador": "Coordenador", "valor_comissao_coordenador": "Valor a pagar"})
        .reset_index(drop=True)
    )
    return {"cen": cen, "gestor": gestor, "coordenador": coordenador}
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_core -v`
Expected: PASS nos 14 testes.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/core/consorcio.py tests/test_consorcio_core.py
git commit -m "feat: regras, status e calculo da comissao de consorcio

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Sugestão de de-para por nome

**Files:**
- Modify: `commission_tool/core/consorcio.py`
- Test: `tests/test_consorcio_core.py`

**Interfaces:**
- Produces: `suggest_cod_vendedor(vendedor_map, manager_relations) -> pd.DataFrame` com as colunas `cd_vendedor_consorcio`, `nm_vendedor_consorcio`, `cod_vendedor`, `sugestao_cod_vendedor`, `sugestao_cen`. A sugestão é só informativa e nunca preenche `cod_vendedor`.

- [ ] **Step 1: Escrever o teste**

Acrescentar em `tests/test_consorcio_core.py` (o import no topo passa a incluir `suggest_cod_vendedor`):

```python
class SuggestionTests(unittest.TestCase):
    def test_suggests_by_exact_name_without_filling_cod_vendedor(self):
        vendedor_map = pd.DataFrame(
            [
                {"cd_vendedor_consorcio": "002697", "nm_vendedor_consorcio": "Marcio Jose Louredo de Bessa", "cod_vendedor": ""},
                {"cd_vendedor_consorcio": "009999", "nm_vendedor_consorcio": "NOME QUE NAO EXISTE", "cod_vendedor": ""},
            ]
        )
        relations = pd.DataFrame(
            [{"cod_vendedor": "A0000E", "vendedor": "MARCIO JOSE LOUREDO DE BESSA", "gerente": "Hernane Borges da Costa"}]
        )

        suggestions = suggest_cod_vendedor(vendedor_map, relations)

        self.assertEqual(suggestions.loc[0, "sugestao_cod_vendedor"], "A0000E")
        self.assertEqual(suggestions.loc[0, "sugestao_cen"], "MARCIO JOSE LOUREDO DE BESSA")
        self.assertEqual(suggestions.loc[0, "cod_vendedor"], "")
        self.assertEqual(suggestions.loc[1, "sugestao_cod_vendedor"], "")
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_core -v`
Expected: FAIL com `ImportError: cannot import name 'suggest_cod_vendedor'`

- [ ] **Step 3: Implementar**

Acrescentar ao final de `commission_tool/core/consorcio.py`:

```python
def suggest_cod_vendedor(
    vendedor_map: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
) -> pd.DataFrame:
    """Suggest an ERP seller code by exact name match. Never fills cod_vendedor."""
    columns = ["cd_vendedor_consorcio", "nm_vendedor_consorcio", "cod_vendedor", "sugestao_cod_vendedor", "sugestao_cen"]
    if vendedor_map is None or vendedor_map.empty:
        return pd.DataFrame(columns=columns)

    result = vendedor_map.copy().reset_index(drop=True)
    result["cod_vendedor"] = result["cod_vendedor"].fillna("").astype(str).str.strip().str.upper()

    by_name: dict[str, tuple[str, str]] = {}
    if manager_relations is not None and not manager_relations.empty:
        relations = manager_relations.copy()
        relations["__nome"] = relations["vendedor"].apply(normalize_text)
        relations = relations[relations["__nome"].ne("")].drop_duplicates(subset=["__nome"], keep="first")
        by_name = {
            row["__nome"]: (normalize_code(row["cod_vendedor"]), str(row["vendedor"]).strip())
            for _, row in relations.iterrows()
        }

    nomes = result["nm_vendedor_consorcio"].apply(normalize_text)
    result["sugestao_cod_vendedor"] = nomes.map(lambda nome: by_name.get(nome, ("", ""))[0])
    result["sugestao_cen"] = nomes.map(lambda nome: by_name.get(nome, ("", ""))[1])
    return result[columns]
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_core -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/core/consorcio.py tests/test_consorcio_core.py
git commit -m "feat: sugestao de de-para de vendedor do consorcio por nome

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Gravação e histórico dos pagamentos de consórcio

**Files:**
- Modify: `commission_tool/data/sources/postgres_consorcio.py`
- Test: `tests/test_postgres_consorcio.py`

**Interfaces:**
- Consumes: DataFrame enriquecido da Task 3.
- Produces:
  - `save_consorcio_paid(df, competence_year: int, competence_month: int, period_label: str) -> int`
  - `read_consorcio_paid_keys() -> dict[str, str]` (chave da cota → competência em que foi paga)
  - `read_consorcio_paid(period_label: str | None = None) -> pd.DataFrame`
  - `read_consorcio_paid_period_labels() -> list[str]`

- [ ] **Step 1: Escrever os testes**

Acrescentar em `tests/test_postgres_consorcio.py` (imports passam a incluir `CONSORCIO_PAID_TABLE`, `_prepare_consorcio_paid`, `read_consorcio_paid_keys`):

```python
class PreparePaidTests(unittest.TestCase):
    def test_keeps_only_apt_rows_and_expected_columns(self):
        df = pd.DataFrame(
            [
                {
                    "arquivo_origem": "Acreuna.csv",
                    "cd_pedido": "4504255710",
                    "cd_grupo": "002362",
                    "cd_cota": "0025",
                    "versao": "00",
                    "parcela": "001 / 003",
                    "st_contemplacao": "Não Contemplada",
                    "dt_alocacao": "21/07/2026",
                    "dt_aprovacao": "27/07/2026",
                    "consorciado": "CONSORCIADO TESTE",
                    "vl_bem": 200000.0,
                    "cd_pdv": "208016",
                    "filial": "Loja Acreuna",
                    "cd_vendedor_consorcio": "002197",
                    "nm_vendedor_consorcio": "VENDEDOR A",
                    "cod_vendedor": "00188",
                    "cen": "VENDEDOR A",
                    "gerente": "Reginaldo Pereira Garcia",
                    "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
                    "percentual_cen": 0.84,
                    "valor_comissao_cen": 1680.0,
                    "percentual_gestor": 0.0,
                    "valor_comissao_gestor": 0.0,
                    "percentual_coordenador": 0.0,
                    "valor_comissao_coordenador": 0.0,
                    "apto": True,
                },
                {
                    "arquivo_origem": "Acreuna.csv",
                    "cd_grupo": "002362",
                    "cd_cota": "0026",
                    "versao": "00",
                    "parcela": "003 / 003",
                    "vl_bem": 200000.0,
                    "apto": False,
                },
            ]
        )

        prepared = _prepare_consorcio_paid(df, 2026, 9, "09/2026")

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared.loc[0, "mes_ano_comissao"], "09/2026")
        self.assertEqual(prepared.loc[0, "competencia_ano"], 2026)
        self.assertEqual(prepared.loc[0, "competencia_mes"], 9)
        self.assertEqual(str(prepared.loc[0, "dt_alocacao"]), "2026-07-21")
        self.assertNotIn("apto", prepared.columns)
        self.assertNotIn("status", prepared.columns)

    def test_paid_keys_query_selects_key_columns(self):
        with patch("commission_tool.data.sources.postgres_consorcio.ensure_consorcio_tables"), patch(
            "commission_tool.data.sources.postgres_consorcio.get_engine",
            return_value=object(),
        ), patch(
            "commission_tool.data.sources.postgres_consorcio.pd.read_sql",
            return_value=pd.DataFrame(
                [
                    {
                        "cd_grupo": "002362",
                        "cd_cota": "0025",
                        "versao": "00",
                        "parcela": "001 / 003",
                        "mes_ano_comissao": "08/2026",
                    }
                ]
            ),
        ) as read_sql:
            keys = read_consorcio_paid_keys()

        self.assertIn(CONSORCIO_PAID_TABLE, str(read_sql.call_args.args[0]))
        self.assertEqual(keys, {"002362||0025||00||001 / 003": "08/2026"})
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_postgres_consorcio -v`
Expected: FAIL com `ImportError: cannot import name '_prepare_consorcio_paid'`

- [ ] **Step 3: Implementar a gravação e a leitura**

Acrescentar ao final de `commission_tool/data/sources/postgres_consorcio.py`:

```python
CONSORCIO_PAID_COLUMNS = [
    "arquivo_origem",
    "cd_pedido",
    "cd_grupo",
    "cd_cota",
    "versao",
    "parcela",
    "st_contemplacao",
    "dt_alocacao",
    "dt_aprovacao",
    "consorciado",
    "vl_bem",
    "cd_pdv",
    "filial",
    "cd_vendedor_consorcio",
    "nm_vendedor_consorcio",
    "cod_vendedor",
    "cen",
    "gerente",
    "coordenador",
    "percentual_cen",
    "valor_comissao_cen",
    "percentual_gestor",
    "valor_comissao_gestor",
    "percentual_coordenador",
    "valor_comissao_coordenador",
]


def _prepare_consorcio_paid(
    df: pd.DataFrame,
    competence_year: int,
    competence_month: int,
    period_label: str,
) -> pd.DataFrame:
    source = df.copy()
    if "apto" in source.columns:
        source = source[source["apto"].fillna(False).astype(bool)]

    prepared = pd.DataFrame(index=range(len(source)))
    source = source.reset_index(drop=True)
    for column in CONSORCIO_PAID_COLUMNS:
        prepared[column] = source[column] if column in source.columns else None

    for column in ["dt_alocacao", "dt_aprovacao"]:
        prepared[column] = pd.to_datetime(prepared[column], dayfirst=True, errors="coerce").dt.date
    for column in [
        "vl_bem",
        "percentual_cen",
        "valor_comissao_cen",
        "percentual_gestor",
        "valor_comissao_gestor",
        "percentual_coordenador",
        "valor_comissao_coordenador",
    ]:
        prepared[column] = pd.to_numeric(prepared[column].apply(parse_br_number), errors="coerce")

    prepared.insert(0, "mes_ano_comissao", period_label)
    prepared.insert(0, "competencia_mes", int(competence_month))
    prepared.insert(0, "competencia_ano", int(competence_year))
    return prepared.reset_index(drop=True)


def save_consorcio_paid(
    df: pd.DataFrame,
    competence_year: int,
    competence_month: int,
    period_label: str,
) -> int:
    """Persist the selected consortium rows. The unique index blocks a repeated cota."""
    ensure_consorcio_tables()
    prepared = _prepare_consorcio_paid(df, competence_year, competence_month, period_label)
    if prepared.empty:
        return 0
    engine = get_engine()
    with engine.begin() as conn:
        prepared.to_sql(CONSORCIO_PAID_TABLE, conn, if_exists="append", index=False)
    return len(prepared)


def read_consorcio_paid_keys() -> dict[str, str]:
    """Map every cota installment already paid to the competence it was paid in."""
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    rows = pd.read_sql(
        text(
            f"""
            SELECT cd_grupo, cd_cota, versao, parcela, mes_ano_comissao
            FROM {CONSORCIO_PAID_TABLE}
            """
        ),
        engine,
    )
    if rows.empty:
        return {}
    keys = (
        rows["cd_grupo"].fillna("").astype(str).str.strip().str.upper()
        + "||"
        + rows["cd_cota"].fillna("").astype(str).str.strip().str.upper()
        + "||"
        + rows["versao"].fillna("").astype(str).str.strip().str.upper()
        + "||"
        + rows["parcela"].fillna("").astype(str).str.split().str.join(" ").str.upper()
    )
    return dict(zip(keys, rows["mes_ano_comissao"].fillna("").astype(str)))


def read_consorcio_paid(period_label: str | None = None) -> pd.DataFrame:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    if period_label:
        query = text(
            f"SELECT * FROM {CONSORCIO_PAID_TABLE} WHERE mes_ano_comissao = :period ORDER BY filial, cen, cd_cota"
        )
        return pd.read_sql(query, engine, params={"period": period_label})
    return pd.read_sql(text(f"SELECT * FROM {CONSORCIO_PAID_TABLE} ORDER BY id"), engine)


def read_consorcio_paid_period_labels() -> list[str]:
    from sqlalchemy import text

    ensure_consorcio_tables()
    engine = get_engine()
    rows = pd.read_sql(
        text(
            f"""
            SELECT DISTINCT competencia_ano, competencia_mes, mes_ano_comissao
            FROM {CONSORCIO_PAID_TABLE}
            ORDER BY competencia_ano DESC, competencia_mes DESC
            """
        ),
        engine,
    )
    return rows["mes_ano_comissao"].fillna("").astype(str).tolist()
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_postgres_consorcio -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/data/sources/postgres_consorcio.py tests/test_postgres_consorcio.py
git commit -m "feat: gravacao e historico dos pagamentos de consorcio

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Seções de Configurações do consórcio

**Files:**
- Create: `commission_tool/ui/consorcio.py`
- Modify: `commission_tool/ui/app.py` (`render_settings_view`, por volta da linha 3375)
- Test: `tests/test_consorcio_ui_helpers.py`

**Interfaces:**
- Produces:
  - `prepare_pdv_editor_df(df) -> pd.DataFrame` com as colunas `cd_pdv`, `nm_pdv`, `filial`.
  - `prepare_vendedor_editor_df(vendedor_map, manager_relations) -> pd.DataFrame` com `cd_vendedor_consorcio`, `nm_vendedor_consorcio`, `cod_vendedor`, `sugestao_cod_vendedor`, `sugestao_cen`, com os pendentes (`cod_vendedor` vazio) no topo.
  - `render_consorcio_settings_sections() -> None` (Streamlit).

- [ ] **Step 1: Escrever o teste dos helpers**

Criar `tests/test_consorcio_ui_helpers.py`:

```python
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_ui_helpers -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'commission_tool.ui.consorcio'`

- [ ] **Step 3: Criar o módulo de UI com os helpers e as seções**

Criar `commission_tool/ui/consorcio.py`:

```python
"""Streamlit views for the consortium commission flow."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from commission_tool.core.consorcio import suggest_cod_vendedor
from commission_tool.data.sources.postgres import read_manager_relations
from commission_tool.data.sources.postgres_consorcio import (
    ensure_consorcio_tables,
    read_consorcio_coordenador,
    read_consorcio_pdv_map,
    read_consorcio_regras,
    read_consorcio_vendedor_map,
    replace_active_consorcio_pdv_map,
    replace_active_consorcio_regras,
    replace_active_consorcio_vendedor_map,
    save_consorcio_coordenador,
)


PDV_EDITOR_COLUMNS = ["cd_pdv", "nm_pdv", "filial"]
VENDEDOR_EDITOR_COLUMNS = [
    "cd_vendedor_consorcio",
    "nm_vendedor_consorcio",
    "cod_vendedor",
    "sugestao_cod_vendedor",
    "sugestao_cen",
]


def prepare_pdv_editor_df(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=PDV_EDITOR_COLUMNS)
    prepared = df.copy()
    for column in PDV_EDITOR_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = ""
    return prepared[PDV_EDITOR_COLUMNS].reset_index(drop=True)


def prepare_vendedor_editor_df(
    vendedor_map: pd.DataFrame | None,
    manager_relations: pd.DataFrame | None,
) -> pd.DataFrame:
    prepared = suggest_cod_vendedor(vendedor_map, manager_relations)
    if prepared.empty:
        return pd.DataFrame(columns=VENDEDOR_EDITOR_COLUMNS)
    prepared["__pendente"] = prepared["cod_vendedor"].eq("")
    prepared = prepared.sort_values(
        ["__pendente", "nm_vendedor_consorcio"],
        ascending=[False, True],
    )
    return prepared[VENDEDOR_EDITOR_COLUMNS].reset_index(drop=True)


def render_consorcio_settings_sections() -> None:
    st.divider()
    st.markdown("### Consórcio: PDV x Filial")
    pdv_file = st.file_uploader(
        "Excel com as colunas CD_Comissionado_PDV, NM_Comissionado_PDV e Filial",
        type=["xlsx"],
        key="consorcio_pdv_upload",
    )
    if pdv_file is not None and st.button("Salvar de-para de PDV", key="consorcio_pdv_save_upload"):
        count = replace_active_consorcio_pdv_map(pd.read_excel(pdv_file))
        st.success(f"{count} PDVs salvos.")

    pdv_df = prepare_pdv_editor_df(read_consorcio_pdv_map())
    edited_pdv = st.data_editor(
        pdv_df,
        num_rows="dynamic",
        width="stretch",
        key="consorcio_pdv_editor",
    )
    if st.button("Salvar alterações de PDV", type="primary", key="consorcio_pdv_save_editor"):
        count = replace_active_consorcio_pdv_map(edited_pdv)
        st.success(f"{count} PDVs salvos.")

    st.divider()
    st.markdown("### Consórcio: Vendedor x CEN")
    st.caption(
        "Os vendedores novos entram automaticamente com o CEN em branco. "
        "A sugestão é apenas informativa: preencha o campo `cod_vendedor` para liberar o pagamento."
    )
    vendedor_df = prepare_vendedor_editor_df(read_consorcio_vendedor_map(), read_manager_relations())
    pendentes = int(vendedor_df["cod_vendedor"].eq("").sum()) if not vendedor_df.empty else 0
    if pendentes:
        st.warning(f"{pendentes} vendedor(es) de consórcio sem CEN definido.")
    edited_vendedor = st.data_editor(
        vendedor_df,
        num_rows="dynamic",
        width="stretch",
        disabled=["sugestao_cod_vendedor", "sugestao_cen"],
        key="consorcio_vendedor_editor",
    )
    if st.button("Salvar alterações de Vendedor x CEN", type="primary", key="consorcio_vendedor_save"):
        count = replace_active_consorcio_vendedor_map(edited_vendedor)
        st.success(f"{count} vendedores salvos.")

    st.divider()
    st.markdown("### Consórcio: regras por parcela")
    regras_df = read_consorcio_regras()
    edited_regras = st.data_editor(
        regras_df,
        num_rows="dynamic",
        width="stretch",
        key="consorcio_regras_editor",
    )
    if st.button("Salvar regras do consórcio", type="primary", key="consorcio_regras_save"):
        count = replace_active_consorcio_regras(edited_regras)
        st.success(f"{count} regras salvas.")

    st.divider()
    st.markdown("### Consórcio: coordenador")
    coordenador = read_consorcio_coordenador()
    nome = st.text_input("Nome do coordenador", value=coordenador.get("nome", ""), key="consorcio_coord_nome")
    email = st.text_input("E-mail do coordenador", value=coordenador.get("email", ""), key="consorcio_coord_email")
    if st.button("Salvar coordenador", type="primary", key="consorcio_coord_save"):
        save_consorcio_coordenador(nome, email)
        st.success("Coordenador salvo.")
```

- [ ] **Step 4: Chamar as seções em Configurações**

Em `commission_tool/ui/app.py`, no topo, acrescentar o import:

```python
from commission_tool.ui.consorcio import render_consorcio_settings_sections
```

E ao final de `render_settings_view`, depois do bloco de "Relação CEN x Gerente":

```python
    render_consorcio_settings_sections()
```

Também, no botão "Garantir tabelas Postgres" dessa mesma tela, acrescentar a chamada de `ensure_consorcio_tables()` ao lado de `ensure_commission_tables()`, importando-a de `commission_tool.data.sources.postgres_consorcio`.

- [ ] **Step 5: Rodar a suíte inteira**

Run: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`
Expected: OK.

- [ ] **Step 6: Commit**

```bash
git add commission_tool/ui/consorcio.py commission_tool/ui/app.py tests/test_consorcio_ui_helpers.py
git commit -m "feat: secoes de configuracao do consorcio

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Tela de apuração do consórcio e exportação

**Files:**
- Create: `commission_tool/io/consorcio_excel.py`
- Modify: `commission_tool/ui/consorcio.py`
- Modify: `commission_tool/ui/app.py` (`render_sidebar` e `main`)
- Test: `tests/test_consorcio_excel.py`

**Interfaces:**
- Produces:
  - `build_consorcio_workbook(base_df, premiacao: dict[str, pd.DataFrame]) -> io.BytesIO` com as abas `Base`, `Premiação CEN`, `Premiação Gestores` e `Premiação Coordenador`.
  - `render_consorcio_apuracao() -> None` (Streamlit).

- [ ] **Step 1: Escrever o teste do workbook**

Criar `tests/test_consorcio_excel.py`:

```python
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_excel -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'commission_tool.io.consorcio_excel'`

- [ ] **Step 3: Implementar o export**

Criar `commission_tool/io/consorcio_excel.py`:

```python
"""Excel export for the consortium commission payout."""

from __future__ import annotations

import io

import pandas as pd

from commission_tool.io.excel import _autosize_columns, _strip_datetime_tz


def build_consorcio_workbook(base_df: pd.DataFrame, premiacao: dict[str, pd.DataFrame]) -> io.BytesIO:
    """Build the workbook that mirrors the manual spreadsheet: Base + three payout tabs."""
    buffer = io.BytesIO()
    sheets = [
        ("Base", base_df),
        ("Premiação CEN", premiacao.get("cen")),
        ("Premiação Gestores", premiacao.get("gestor")),
        ("Premiação Coordenador", premiacao.get("coordenador")),
    ]
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, frame in sheets:
            data = _strip_datetime_tz(frame if frame is not None else pd.DataFrame())
            data.to_excel(writer, sheet_name=sheet_name, index=False)
            _autosize_columns(writer.sheets[sheet_name])
    buffer.seek(0)
    return buffer
```

- [ ] **Step 4: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_consorcio_excel -v`
Expected: PASS.

- [ ] **Step 5: Implementar a tela de apuração**

Acrescentar em `commission_tool/ui/consorcio.py` (e completar os imports com `datetime.date`, `read_consorcio_csv`, `ConsorcioFileError`, `enrich_consorcio_rows`, `build_premiacao_tables`, `STATUS_APTO`, `register_missing_consorcio_vendedores`, `read_consorcio_paid_keys`, `save_consorcio_paid`, `build_consorcio_workbook`):

```python
def render_consorcio_apuracao() -> None:
    st.markdown("## Apuração de Consórcio")
    ensure_consorcio_tables()

    hoje = date.today()
    col_ano, col_mes = st.columns(2)
    ano = col_ano.number_input("Ano da competência", min_value=2020, max_value=2100, value=hoje.year, step=1)
    mes = col_mes.number_input("Mês da competência", min_value=1, max_value=12, value=hoje.month, step=1)
    period_label = f"{int(mes):02d}/{int(ano)}"

    uploads = st.file_uploader(
        "Relatórios do sistema de consórcio (um CSV por loja)",
        type=["csv"],
        accept_multiple_files=True,
        key="consorcio_uploads",
    )
    if not uploads:
        st.info("Envie os arquivos para iniciar a apuração.")
        return

    frames = []
    for uploaded in uploads:
        try:
            frames.append(read_consorcio_csv(uploaded, file_name=uploaded.name))
            st.success(f"{uploaded.name}: {len(frames[-1])} linhas lidas.")
        except ConsorcioFileError as exc:
            st.error(f"{uploaded.name}: {exc}")
    if not frames:
        return

    raw = pd.concat(frames, ignore_index=True)
    novos = register_missing_consorcio_vendedores(raw)
    if novos:
        st.warning(
            f"{novos} vendedor(es) de consórcio novo(s) cadastrado(s) sem CEN. "
            "Preencha em Configurações para liberar o pagamento."
        )

    rows = enrich_consorcio_rows(
        raw,
        pdv_map=read_consorcio_pdv_map(),
        vendedor_map=read_consorcio_vendedor_map(),
        manager_relations=read_manager_relations(),
        regras=read_consorcio_regras(),
        coordenador=read_consorcio_coordenador(),
        paid_keys=read_consorcio_paid_keys(),
    )

    aptos = rows[rows["apto"]]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Linhas", len(rows))
    col2.metric("CEN", f"R$ {aptos['valor_comissao_cen'].sum():,.2f}")
    col3.metric("Gestores", f"R$ {aptos['valor_comissao_gestor'].sum():,.2f}")
    col4.metric("Coordenador", f"R$ {aptos['valor_comissao_coordenador'].sum():,.2f}")

    bloqueadas = rows[~rows["apto"] & rows["status"].ne("Sem comissão")]
    if not bloqueadas.empty:
        with st.expander(f"{len(bloqueadas)} linha(s) bloqueada(s)", expanded=True):
            st.dataframe(bloqueadas["status"].value_counts().rename("Linhas"), width="stretch")

    editor_df = rows.copy()
    editor_df.insert(0, "Selecionar", editor_df["apto"])
    edited = st.data_editor(
        editor_df,
        width="stretch",
        disabled=[column for column in editor_df.columns if column != "Selecionar"],
        key="consorcio_apuracao_editor",
    )
    selecionadas = edited[edited["Selecionar"].fillna(False) & edited["apto"]].drop(columns=["Selecionar"])

    premiacao = build_premiacao_tables(selecionadas)
    st.markdown("### Premiação a pagar")
    st.dataframe(premiacao["cen"], width="stretch")
    st.dataframe(premiacao["gestor"], width="stretch")
    st.dataframe(premiacao["coordenador"], width="stretch")

    st.download_button(
        "Exportar Excel",
        data=build_consorcio_workbook(selecionadas, premiacao),
        file_name=f"consorcio_{int(ano)}_{int(mes):02d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )

    confirmar = st.checkbox(
        f"Confirmo o pagamento de {len(selecionadas)} linha(s) na competência {period_label}",
        key="consorcio_confirmar",
    )
    if st.button("Registrar pagamento", type="primary", disabled=not confirmar, width="stretch"):
        gravadas = save_consorcio_paid(selecionadas, int(ano), int(mes), period_label)
        st.success(f"{gravadas} linha(s) gravada(s) em {period_label}.")
```

- [ ] **Step 6: Ligar no menu**

Em `commission_tool/ui/app.py`, no `st.radio` de `render_sidebar`, acrescentar `"Apuração Consórcio"` logo depois de `"Apurações"`. Em `main`, acrescentar o roteamento, importando `render_consorcio_apuracao` de `commission_tool.ui.consorcio`:

```python
    elif selected_view == "Apuração Consórcio":
        render_consorcio_apuracao()
```

- [ ] **Step 7: Rodar a suíte inteira**

Run: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`
Expected: OK.

- [ ] **Step 8: Commit**

```bash
git add commission_tool/io/consorcio_excel.py commission_tool/ui/consorcio.py commission_tool/ui/app.py tests/test_consorcio_excel.py
git commit -m "feat: tela de apuracao de consorcio com exportacao

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Consórcio nos relatórios

**Files:**
- Modify: `commission_tool/core/reports.py`
- Test: `tests/test_reports.py`

**Interfaces:**
- Produces:
  - `build_cen_report(paid_commissions, manager_relations, period_label, consorcio_paid=None)` passa a ter a coluna `Consórcio` antes de `Valor Comissão Total`, somada ao total.
  - `build_manager_report(paid_commissions, manager_relations, period_label, consorcio_paid=None)` passa a ter a coluna `Gestor Consórcio` (valor gravado, fora da regra de 33%) e `Comissão Gerente` passa a ser 33% da comissão de máquinas mais o valor de consórcio.
  - `build_consorcio_coordinator_report(consorcio_paid, period_label) -> pd.DataFrame` com `Nome`, `Receita (VL Bem)`, `Valor Comissão`.

- [ ] **Step 1: Escrever os testes**

Acrescentar em `tests/test_reports.py` (imports passam a incluir `build_consorcio_coordinator_report`):

```python
CONSORCIO_PAID = pd.DataFrame(
    [
        {
            "mes_ano_comissao": "09/2026",
            "cod_vendedor": "00188",
            "cen": "VENDEDOR A",
            "filial": "Loja Acreuna",
            "gerente": "Reginaldo Pereira Garcia",
            "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
            "vl_bem": 200000.0,
            "valor_comissao_cen": 1680.0,
            "valor_comissao_gestor": 0.0,
            "valor_comissao_coordenador": 0.0,
        },
        {
            "mes_ano_comissao": "09/2026",
            "cod_vendedor": "00188",
            "cen": "VENDEDOR A",
            "filial": "Loja Acreuna",
            "gerente": "Reginaldo Pereira Garcia",
            "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
            "vl_bem": 206994.15,
            "valor_comissao_cen": 0.0,
            "valor_comissao_gestor": 434.687715,
            "valor_comissao_coordenador": 310.491225,
        },
        {
            "mes_ano_comissao": "08/2026",
            "cod_vendedor": "00188",
            "cen": "VENDEDOR A",
            "filial": "Loja Acreuna",
            "gerente": "Reginaldo Pereira Garcia",
            "coordenador": "DJALMA RODRIGUES DE PAULA JUNIOR",
            "vl_bem": 100000.0,
            "valor_comissao_cen": 840.0,
            "valor_comissao_gestor": 0.0,
            "valor_comissao_coordenador": 0.0,
        },
    ]
)


class ConsorcioInReportsTests(unittest.TestCase):
    def test_cen_report_has_consorcio_column_for_the_period(self):
        relations = pd.DataFrame(
            [{"cod_vendedor": "00188", "vendedor": "VENDEDOR A", "gerente": "Reginaldo Pereira Garcia"}]
        )

        report, _ = build_cen_report(
            pd.DataFrame(),
            relations,
            "09/2026",
            consorcio_paid=CONSORCIO_PAID,
        )

        row = report.loc[report["CEN"] == "VENDEDOR A"].iloc[0]
        self.assertAlmostEqual(row["Consórcio"], 1680.0, places=2)
        self.assertAlmostEqual(row["Valor Comissão Total"], 1680.0, places=2)

    def test_manager_report_adds_consorcio_outside_the_33_percent(self):
        paid = pd.DataFrame(
            [
                {
                    "mes_ano_comissao": "09/2026",
                    "gerente": "Reginaldo Pereira Garcia",
                    "cod_vendedor": "00188",
                    "cen": "VENDEDOR A",
                    "filial": "Loja Acreuna",
                    "classificacao_venda": "Venda Direta",
                    "receita_bruta": 100000.0,
                    "receita_bruta_incentivos_rs": 100000.0,
                    "margem_incentivos_rs": 10000.0,
                    "perc_margem_bruta": 10.0,
                    "valor_comissao_fat": 6000.0,
                    "valor_comissao_margem": 0.0,
                    "valor_comissao_total": 6000.0,
                }
            ]
        )

        report = build_manager_report(paid, None, "09/2026", consorcio_paid=CONSORCIO_PAID)

        row = report.iloc[0]
        self.assertAlmostEqual(row["Gestor Consórcio"], 434.687715, places=6)
        self.assertAlmostEqual(row["Comissão Gerente"], 6000.0 * 0.33 + 434.687715, places=6)

    def test_coordinator_report_sums_only_the_period(self):
        report = build_consorcio_coordinator_report(CONSORCIO_PAID, "09/2026")

        self.assertEqual(report.loc[0, "Nome"], "DJALMA RODRIGUES DE PAULA JUNIOR")
        self.assertAlmostEqual(report.loc[0, "Valor Comissão"], 310.491225, places=6)
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_reports -v`
Expected: FAIL com `TypeError: build_cen_report() got an unexpected keyword argument 'consorcio_paid'`

- [ ] **Step 3: Implementar**

Em `commission_tool/core/reports.py`, acrescentar o helper e alterar os dois relatórios:

```python
def _prepare_consorcio_for_period(
    consorcio_paid: pd.DataFrame | None,
    period_label: str,
) -> pd.DataFrame:
    if consorcio_paid is None or consorcio_paid.empty:
        return pd.DataFrame()

    rows = consorcio_paid.copy()
    rows = rows[
        rows.get("mes_ano_comissao", pd.Series(dtype=object)).fillna("").astype(str).eq(str(period_label))
    ].copy()
    if rows.empty:
        return rows

    rows["__cod_vendedor"] = rows.get("cod_vendedor", pd.Series(dtype=object)).apply(_normalize_code)
    rows["__gerente"] = rows.get("gerente", pd.Series(dtype=object)).apply(_normalize_text)
    for column in ["vl_bem", "valor_comissao_cen", "valor_comissao_gestor", "valor_comissao_coordenador"]:
        rows[column] = pd.to_numeric(
            rows[column] if column in rows.columns else pd.Series(index=rows.index, dtype=float),
            errors="coerce",
        ).fillna(0.0)
    return rows


def build_consorcio_coordinator_report(
    consorcio_paid: pd.DataFrame | None,
    period_label: str,
) -> pd.DataFrame:
    report_columns = ["Nome", "Receita (VL Bem)", "Valor Comissão"]
    rows = _prepare_consorcio_for_period(consorcio_paid, period_label)
    if rows.empty:
        return pd.DataFrame(columns=report_columns)

    rows = rows[rows["valor_comissao_coordenador"].ne(0)]
    if rows.empty:
        return pd.DataFrame(columns=report_columns)

    grouped = (
        rows.groupby(rows.get("coordenador", pd.Series(dtype=object)).fillna("").astype(str).str.strip())
        .agg({"vl_bem": "sum", "valor_comissao_coordenador": "sum"})
        .reset_index()
    )
    grouped.columns = report_columns
    return grouped
```

Em `build_cen_report`, a assinatura ganha `consorcio_paid: pd.DataFrame | None = None`, a lista `report_columns` ganha `"Consórcio"` logo antes de `"Valor Comissão Total"`, e, depois do merge com `aggregated` (e também nos dois retornos antecipados que zeram as colunas), somar por CEN:

```python
    consorcio = _prepare_consorcio_for_period(consorcio_paid, period_label)
    if consorcio.empty:
        report["Consórcio"] = 0.0
    else:
        consorcio_por_cen = (
            consorcio.groupby("__cod_vendedor")["valor_comissao_cen"].sum().rename("Consórcio").reset_index()
        )
        report = report.merge(consorcio_por_cen, on="__cod_vendedor", how="left")
        report["Consórcio"] = pd.to_numeric(report["Consórcio"], errors="coerce").fillna(0.0)
```

Esse bloco precisa rodar **antes** de `report.drop(columns=["__cod_vendedor"])`, e `"Consórcio"` entra na soma do `Valor Comissão Total`.

Em `build_manager_report`, a assinatura ganha `consorcio_paid: pd.DataFrame | None = None`, `report_columns` ganha `"Gestor Consórcio"` antes de `"Comissão Gerente"`, e dentro do laço por gerente:

```python
        consorcio_gestor = 0.0
        if not consorcio.empty:
            consorcio_gestor = float(
                consorcio.loc[consorcio["__gerente"].eq(manager_key), "valor_comissao_gestor"].sum()
            )
```

com `consorcio = _prepare_consorcio_for_period(consorcio_paid, period_label)` calculado uma vez antes do laço, e as duas colunas no dicionário da linha:

```python
                "Gestor Consórcio": consorcio_gestor,
                "Comissão Gerente": commission_total_cen * 0.33 + consorcio_gestor,
```

Um gerente que tenha apenas consórcio no mês também precisa aparecer. Depois do laço, antes do `return`:

```python
    ja_listados = {_normalize_text(row["Gerente"]) for row in rows}
    if not consorcio.empty:
        for manager_key, manager_consorcio in consorcio.groupby("__gerente", dropna=False):
            if not manager_key or manager_key in ja_listados:
                continue
            gerente_display = (
                manager_consorcio.get("gerente", pd.Series(dtype=object))
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
            )
            consorcio_gestor = float(manager_consorcio["valor_comissao_gestor"].sum())
            rows.append(
                {
                    "Gerente": gerente_display.iloc[0] if not gerente_display.empty else str(manager_key),
                    "Receita Bruta": 0.0,
                    "Margem + Incentivos R$": 0.0,
                    "% Margem Bruta": 0.0,
                    "Comissão Total CEN": 0.0,
                    "Gestor Consórcio": consorcio_gestor,
                    "Comissão Gerente": consorcio_gestor,
                }
            )
```

Esse bloco também exige que o `return` final continue usando `pd.DataFrame(rows, columns=report_columns)`, para que a ordem das colunas não dependa da ordem de inserção.

- [ ] **Step 4: Rodar e ver passar**

Run: `.venv/Scripts/python -m unittest tests.test_reports -v`
Expected: PASS, inclusive nos testes antigos de `build_cen_report` e `build_manager_report`, que chamam as funções sem o novo argumento.

- [ ] **Step 5: Ligar na tela de Relatórios**

Em `commission_tool/ui/app.py`, em `render_reports_view`, carregar `read_consorcio_paid(period_label)` e repassar como `consorcio_paid=` nas chamadas de `build_cen_report` e `build_manager_report`, além de exibir `build_consorcio_coordinator_report` num quadro próprio, no mesmo padrão visual do quadro do coordenador de implementos.

- [ ] **Step 6: Rodar a suíte inteira e commitar**

Run: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`
Expected: OK.

```bash
git add commission_tool/core/reports.py commission_tool/ui/app.py tests/test_reports.py
git commit -m "feat: consorcio nos relatorios de CEN, gerente e coordenador

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Consórcio nos e-mails

**Files:**
- Modify: `commission_tool/core/report_email.py`
- Modify: `commission_tool/ui/app.py` (`render_report_email_section`, `build_report_email_attachments`)
- Test: `tests/test_report_email.py`

**Interfaces:**
- Produces: `build_report_email_plan(...)` ganha os parâmetros `consorcio_by_cod_vendedor: dict[str, pd.DataFrame] | None = None`, `consorcio_coordinator_df: pd.DataFrame | None = None`, `consorcio_coordinator_email: str = ""` e `consorcio_coordinator_name: str = ""`. Cada job de CEN e de gerente ganha um anexo extra `("Consórcio", df)` quando houver consórcio no período; um job `kind="consorcio_coordinator"` é criado para o coordenador.

- [ ] **Step 1: Escrever os testes**

Acrescentar em `tests/test_report_email.py`:

```python
    def test_cen_job_gets_consorcio_attachment(self):
        consorcio = {
            "AAETBU": pd.DataFrame([{"Cota": "0025", "Parcela": "001 / 003", "Valor": 1680.0}])
        }

        plan = build_report_email_plan(
            [("ABNER SAMUEL FERREIRA COSTA", "AAETBU", pd.DataFrame([{"Nro Documento": "1"}]))],
            self.relations,
            "09/2026",
            mode="test",
            test_recipient="teste@hohl.com.br",
            production_cc="",
            consorcio_by_cod_vendedor=consorcio,
        )

        cen_job = next(job for job in plan.jobs if job.kind == "cen")
        self.assertIn("Consórcio", [name for name, _ in cen_job.attachments])

    def test_consorcio_coordinator_gets_its_own_job(self):
        plan = build_report_email_plan(
            [],
            self.relations,
            "09/2026",
            mode="production",
            test_recipient="",
            production_cc="",
            consorcio_coordinator_df=pd.DataFrame([{"Nome": "DJALMA", "Valor Comissão": 310.49}]),
            consorcio_coordinator_email="djalma.junior@hohl.com.br",
            consorcio_coordinator_name="DJALMA RODRIGUES DE PAULA JUNIOR",
        )

        job = next(job for job in plan.jobs if job.kind == "consorcio_coordinator")
        self.assertEqual(job.to, "djalma.junior@hohl.com.br")
        self.assertIn("Consórcio", job.subject)
```

Nota para quem implementa: `self.relations` é o DataFrame de relações usado pelos testes existentes desse arquivo. Reaproveitar o mesmo fixture (ou o `setUp`) já presente na classe; se os testes atuais montam o DataFrame inline, extrair para `setUp` como primeiro passo, sem mudar os valores.

- [ ] **Step 2: Rodar e ver falhar**

Run: `.venv/Scripts/python -m unittest tests.test_report_email -v`
Expected: FAIL com `TypeError: build_report_email_plan() got an unexpected keyword argument 'consorcio_by_cod_vendedor'`

- [ ] **Step 3: Implementar**

Em `commission_tool/core/report_email.py`:

- `Literal` de `ReportEmailJob.kind` passa a incluir `"consorcio_coordinator"`.
- `build_report_email_plan` ganha os quatro parâmetros novos.
- No laço dos CENs, ao montar o `ReportEmailJob` de CEN, acrescentar o anexo quando houver dados:

```python
        consorcio_df = (consorcio_by_cod_vendedor or {}).get(report.cod_vendedor)
        cen_attachments = (
            [("Consórcio", consorcio_df)] if consorcio_df is not None and not consorcio_df.empty else []
        )
```

e passar `attachments=cen_attachments` no job de CEN.

- No laço dos gerentes, concatenar os DataFrames de consórcio dos CENs daquele gerente e passar como `attachments=[("Consórcio", pd.concat(...))]` quando houver.
- Depois do bloco do coordenador de implementos, acrescentar o job do coordenador de consórcio:

```python
    if consorcio_coordinator_df is not None and not consorcio_coordinator_df.empty:
        actual_email = _clean_email(consorcio_coordinator_email)
        if mode == "production" and not actual_email:
            issues.append(f"Coordenador de consórcio {consorcio_coordinator_name} sem e-mail.")
        else:
            jobs.append(
                ReportEmailJob(
                    kind="consorcio_coordinator",
                    display_name=consorcio_coordinator_name or "(sem nome)",
                    to=_job_recipient(mode, actual_email, test_recipient),
                    cc=cc,
                    subject=f"Premiação de Consórcio - {period_label}",
                    body=(
                        f"Olá,\n\nSegue em anexo a premiação de consórcio referente à "
                        f"competência {period_label}.\n\nAtenciosamente."
                    ),
                    reports=[],
                    attachments=[("Premiação Consórcio", consorcio_coordinator_df)],
                )
            )
```

Em `commission_tool/ui/app.py`, `render_report_email_section` passa a montar `consorcio_by_cod_vendedor` a partir de `read_consorcio_paid(period_label)`, agrupando por `cod_vendedor`, e repassa o coordenador vindo de `read_consorcio_coordenador()`. `build_report_email_attachments` já trata `job.attachments`, então nada muda ali.

- [ ] **Step 4: Rodar a suíte inteira**

Run: `.venv/Scripts/python -m unittest discover -s tests -p "test_*.py"`
Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add commission_tool/core/report_email.py commission_tool/ui/app.py tests/test_report_email.py
git commit -m "feat: consorcio nos e-mails de CEN, gerente e coordenador

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Conferência contra a planilha de agosto

**Files:**
- Create: `scripts/conferir_consorcio_agosto.py`
- Modify: `docs/regras_calculo.md`

**Interfaces:**
- Consumes: `read_consorcio_csv`, `enrich_consorcio_rows`, `build_premiacao_tables`.
- Produces: script de linha de comando que imprime os três totais e compara com os valores esperados.

- [ ] **Step 1: Escrever o script**

Criar `scripts/conferir_consorcio_agosto.py`:

```python
"""Confere a apuração de consórcio contra a planilha manual de agosto/2026.

Uso:
    python scripts/conferir_consorcio_agosto.py --pasta "<caminho da pasta Relatorios>"

O script não grava nada: apenas lê os CSV, aplica as regras e compara os
totais com os da planilha `Relatorio Cotas Consórcio Agosto 26.xlsx`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env", encoding="utf-8")
except ImportError:
    pass

import pandas as pd

from commission_tool.core.consorcio import build_premiacao_tables, enrich_consorcio_rows
from commission_tool.data.sources.postgres import read_manager_relations
from commission_tool.data.sources.postgres_consorcio import (
    read_consorcio_coordenador,
    read_consorcio_pdv_map,
    read_consorcio_regras,
    read_consorcio_vendedor_map,
)
from commission_tool.io.consorcio_csv import read_consorcio_csv

ESPERADO = {"cen": 112560.00, "gestor": 8456.564865, "coordenador": 6040.403475}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pasta", required=True, help="Pasta com os CSV do mês")
    args = parser.parse_args()

    arquivos = sorted(Path(args.pasta).glob("*.csv"))
    if not arquivos:
        raise SystemExit(f"Nenhum CSV encontrado em {args.pasta}")

    frames = []
    for arquivo in arquivos:
        with arquivo.open("rb") as handle:
            frames.append(read_consorcio_csv(handle, file_name=arquivo.name))
        print(f"{arquivo.name}: {len(frames[-1])} linhas")

    rows = enrich_consorcio_rows(
        pd.concat(frames, ignore_index=True),
        pdv_map=read_consorcio_pdv_map(),
        vendedor_map=read_consorcio_vendedor_map(),
        manager_relations=read_manager_relations(),
        regras=read_consorcio_regras(),
        coordenador=read_consorcio_coordenador(),
        paid_keys=None,
    )

    bloqueadas = rows[~rows["apto"]]
    if not bloqueadas.empty:
        print("\nLinhas não aptas por status:")
        print(bloqueadas["status"].value_counts().to_string())

    tabelas = build_premiacao_tables(rows)
    totais = {chave: float(tabela["Valor a pagar"].sum()) for chave, tabela in tabelas.items()}

    print("\nTotais apurados x planilha:")
    for chave, esperado in ESPERADO.items():
        apurado = totais.get(chave, 0.0)
        marca = "OK" if abs(apurado - esperado) < 0.01 else "DIVERGENTE"
        print(f"  {chave:12s} apurado={apurado:14,.2f}  planilha={esperado:14,.2f}  {marca}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Rodar a conferência**

Run:

```bash
.venv/Scripts/python scripts/conferir_consorcio_agosto.py --pasta "C:/Users/leonardo.mota/Grupo Luiz Hohl/Planejamento Economico - Documents/Planejamento Economico/1 - Gestão/28 - COMISSÕES/FY26/Calculo/AGO.26/Consórcios/Relatorios"
```

Expected: as três linhas com `OK`. Se alguma vier `DIVERGENTE`, investigar com as linhas não aptas impressas acima: na primeira execução é esperado que os de-paras de PDV e de vendedor estejam incompletos, o que bloqueia linhas e reduz os totais. Preencher em Configurações e rodar de novo.

- [ ] **Step 3: Documentar a regra**

Em `docs/regras_calculo.md`, acrescentar ao final:

```markdown
## Consórcio

Vigente a partir de 09/2026, implementado em `commission_tool/core/consorcio.py`.

- Entrada: CSV do sistema de consórcio, um por PDV, com upload na tela "Apuração Consórcio".
- Base de cálculo: `VL_Bem`. O `VL_Comissao` do arquivo é informativo.
- Percentuais por parcela, editáveis em Configurações (`comissao_consorcio_regras`):
  parcela 001 paga 0,84% ao CEN; parcela 002 paga 0,21% ao gestor e 0,15% ao coordenador;
  parcela 003 não paga ninguém.
- A comissão do gestor no consórcio é um percentual sobre o bem, e **não** os 33% usados em máquinas.
- Chave única de pagamento: grupo + cota + versão + parcela.
- Linha sem de-para de PDV ou de vendedor fica bloqueada e nunca é paga por suposição.
```

- [ ] **Step 4: Commit**

```bash
git add scripts/conferir_consorcio_agosto.py docs/regras_calculo.md
git commit -m "feat: script de conferencia do consorcio contra agosto/2026

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Verificação final (manual, com o usuário)

1. Em Configurações, carregar o de-para de PDV a partir de `backups/de_para_comissao_consorcio.xlsx` e acrescentar o PDV `002001` (Goiânia).
2. Subir os 5 CSV de agosto na tela "Apuração Consórcio" e preencher os `cod_vendedor` pendentes em Configurações.
3. Conferir os três totais contra a planilha: R$ 112.560,00, R$ 8.456,56 e R$ 6.040,40.
4. Registrar o pagamento e subir os mesmos arquivos de novo: todas as linhas devem vir como "já pago em 08/2026".
5. Em Relatórios, conferir a coluna Consórcio no relatório por CEN, a coluna Gestor Consórcio no relatório por gerente e o quadro do coordenador.
