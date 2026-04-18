# Regras de Validação para Pagamento

> Status: regra funcional migrada para `commission_tool/core/eligibility.py`.

## V1 - Validação de Incentivo

Aplica-se apenas a classificações com exigência de chassi:

- `Maquinas JD - Novos`

Fluxo:

1. Buscar `Nro Chassi` em `bdnIncentivos`.
2. Obter `[Nota Fiscal Número]` do incentivo.
3. Buscar saldo em `bdnContasReceber` pelo título da NF de incentivo.
4. Se saldo absoluto menor que R$ 0,01, marcar `APTO`.
5. Se houver saldo pendente, marcar `NÃO APTO`.
6. Se chassi/NF/título não forem localizados, marcar `NÃO ENCONTRADO`.

## V2 - Validação de Pagamento do Cliente

Fluxo:

1. Buscar `Nro Documento` em `bdnFaturamento`.
2. Obter `[Cliente Código]`, `[Data de Emissão]` e `[Nota Fiscal Número]`.
3. Buscar saldo em `bdnContasReceber` por cliente + título.
4. Se saldo absoluto menor que R$ 0,01, marcar `APTO`.
5. Se houver saldo pendente apenas em tipo `BL`, marcar `APTO`.
6. Se houver saldo pendente em qualquer outro tipo, marcar `NÃO APTO`.
7. Se a data de emissão divergir, marcar `ATENÇÃO` quando a condição financeira estiver apta.

## Status Geral

- `✅ APTO`: V1 e V2 estão `APTO` ou `N/A`.
- `❌ NÃO APTO`: qualquer validação está `NÃO APTO`.
- `⚠️ VERIFICAR`: qualquer validação está `ERRO`, `NÃO ENCONTRADO` ou `ATENÇÃO`.

## Validação de Títulos de Incentivo

Além do valor agregado de incentivo por chassi, o sistema mantém uma tabela separada com os títulos de incentivo.

Regra:

1. Buscar títulos em `bdnIncentivos.[Nota Fiscal Número]`.
2. Agrupar por `Chassi` e título, somando `Valor Incentivo`.
3. Consultar cada título em `bdnContasReceber.[Título Número]`.
4. Somar `Valor Saldo`.
5. Se houver múltiplos títulos por chassi, todos devem ser considerados.
6. Saldo total zero: incentivo `APTO`.
7. Saldo total maior que zero: incentivo `NÃO APTO`.
8. Título ausente em `bdnContasReceber`: incentivo `VERIFICAR`.
