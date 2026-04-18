# Persistência no Postgres

> Status: estrutura inicial criada.

O Postgres será usado para guardar dados de auditoria e configurações que não devem ficar apenas na tabela temporária de apuração.

## Variáveis de Ambiente

O sistema aceita uma URL direta:

- `POSTGRES_URL`

Ou os campos separados:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_SSLMODE`

## Tabela `comissoespagas`

Armazena todas as linhas de comissão pagas, sejam elas selecionadas na tela de apuração ou importadas por Excel histórico.

Uso:

- Auditoria de pagamentos.
- Consulta por mês/ano no sidebar do Streamlit.
- Registro de fonte (`streamlit` ou `upload_excel`).
- Registro de período de apuração.

Campos principais:

- `competencia_ano`
- `competencia_mes`
- `mes_ano_comissao`
- `periodo_inicio`
- `periodo_fim`
- `fonte`
- `arquivo_origem`
- `tipo`
- `filial`
- `data_emissao`
- `nro_documento`
- `modelo`
- `nro_chassi`
- `nome_cliente`
- `cen`
- `classificacao_venda`
- valores de receita, margem, incentivos e comissão

## Tabela `comissao_faturamento_modelo`

Armazena o percentual de comissão sobre faturamento por modelo.

Campos:

- `modelo`
- `percentual_comissao_fat`
- `ativo`
- `updated_at`

## Tabela `comissao_margem_modelo`

Armazena o percentual de comissão sobre margem por modelo.

Campos:

- `modelo`
- `percentual_comissao_margem`
- `meta_margem`
- `ativo`
- `updated_at`

`meta_margem` fica nesta tabela para manter as três tabelas solicitadas, sem criar uma quarta tabela apenas para metas.

