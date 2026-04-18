# Validador de Comissões — Grupo Luiz Hohl

## Pré-requisitos

1. Python 3.9+
2. ODBC Driver 17 for SQL Server instalado na máquina
   - Windows: https://aka.ms/sqlserverodbc
   - Linux: `sudo apt install msodbcsql17`

## Instalação

```bash
pip install -r requirements.txt
```

## Execução

```bash
streamlit run app.py
```

## Uso Atual

1. Abra http://localhost:8501 no navegador.
2. Na barra lateral, configure a conexão SQL Server.
3. Clique em **Testar Conexão**.
4. Faça upload do arquivo Excel de comissões na aba `Analitico CEN`.
5. Clique em **Executar Validação Completa**.
6. Analise os resultados e exporte conforme necessário.

## Estrutura Atual

O `app.py` da raiz é apenas o entrypoint Streamlit. A lógica principal foi separada em:

- `commission_tool/data/sources/sqlserver.py`: conexão e consultas SQL Server / BDN.
- `commission_tool/data/sources/postgres.py`: persistência opcional de tabelas derivadas no Postgres.
- `commission_tool/core/eligibility.py`: regras de validação para pagamento.
- `commission_tool/io/excel.py`: leitura da planilha temporária e exportação Excel.
- `commission_tool/ui/app.py`: interface Streamlit.
- `commission_tool/core/calculator.py`: fronteira do cálculo de comissão, pendente das regras.
- `commission_tool/data/pipeline.py`: fronteira do pipeline de extração/normalização, pendente do mapeamento BDN.

## Testes

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Tabelas SQL Server Esperadas na Validação

| Tabela | Campos utilizados |
|---|---|
| `bdnIncentivos` | `[Chassi]`, `[Nota Fiscal Número]` |
| `bdnFaturamento` | `[Nota Fiscal Número]`, `[Data de Emissão]`, `[Cliente Código]` |
| `bdnContasReceber` | `[Título Número]`, `[Cliente Código]`, `[Valor Saldo]`, `[Tipo Título]`, `[Data de Emissão]` |

## Documentos de Regra

- `docs/commission_tool_spec.md`: visão geral e arquitetura alvo.
- `docs/regras_extracao.md`: perguntas e contrato inicial da extração SQL Server.
- `docs/regras_calculo.md`: fonte de verdade futura para cálculo de comissão.
- `docs/regras_configuracao_modelos.md`: configuração futura de percentuais e metas por modelo.
- `docs/regras_postgres.md`: tabelas Postgres para pagamentos e percentuais por modelo.
- `docs/regras_validacao_pagamento.md`: regras atualmente implementadas para liberar pagamento.

## Postgres

Configure no `.env` uma `POSTGRES_URL` ou as variáveis separadas `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` e `POSTGRES_SSLMODE`.

Tabelas criadas pelo sistema:

- `comissoespagas`
- `comissao_faturamento_modelo`
- `comissao_margem_modelo`
