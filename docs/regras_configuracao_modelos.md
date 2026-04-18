# Configuração de Regras por Modelo

> Status: pendente da planilha de percentuais.

Esta tabela será usada para parametrizar percentuais e metas por `Modelo`.

## Colunas Previstas

- `Modelo`
- `% Comissão Fat.`
- `Meta de Margem`
- `% Comissão Margem`

## Comportamento Atual

Enquanto a planilha de configuração não for fornecida:

- `% Comissão Fat.` fica `0`
- `Meta de Margem` fica `0`
- `% Comissão Margem` fica `0`
- `Valor Comissão Fat.` fica `0`
- `Valor Comissão Margem` fica `0`
- `Valor Comissão Total` fica `0`

## Chave

A chave de aplicação será `Modelo`, vindo de `bdnVeiculos.[Veículo Modelo]`.

## Persistência

As configurações serão salvas no Postgres em duas tabelas:

- `comissao_faturamento_modelo`
- `comissao_margem_modelo`

Como o usuário solicitou três tabelas no total, a meta de margem fica na tabela `comissao_margem_modelo`.
