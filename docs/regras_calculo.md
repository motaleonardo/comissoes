# Regras de Cálculo de Comissão

> Status: pendente de definição. Este arquivo será a fonte de verdade para implementar `commission_tool/core/calculator.py`.

## Contrato do Cálculo

Entrada esperada: DataFrame normalizado de vendas gerado pelo pipeline de extração.

Saída esperada:

- `vendedor_id`
- `vendedor_nome`
- `periodo`
- `filial`
- `documento`
- `classificacao_venda`
- `base_calculo`
- `percentual_aplicado`
- `comissao_bruta`
- `ajustes`
- `comissao_liquida`
- `regra_aplicada`

## Regras a Definir

- Percentual padrão por classificação de venda.
- Regras por cargo ou tipo de vendedor.
- Base de cálculo: valor da NF, margem, lucro bruto, recebimento ou outro indicador.
- Metas mínimas para liberar comissão.
- Faixas progressivas ou percentuais fixos.
- Deduções, estornos e ajustes manuais.
- Tratamento de margem negativa.
- Tratamento de devolução/cancelamento.
- Arredondamento e casas decimais.

## Configuração por Modelo

Para máquinas, os seguintes campos serão configuráveis por `Modelo`:

- `% Comissão Fat.`
- `Meta de Margem`
- `% Comissão Margem`

Enquanto a planilha de configuração não for fornecida, a extração traz todos esses campos como `0`.

## Perguntas Abertas

- Existe uma comissão por NF e outra por margem?
- A meta de margem altera percentual ou apenas bloqueia pagamento?
- Comissão é calculada no mês da venda ou no mês do pagamento do cliente?
- Existe teto ou piso de comissão?
- Existe aprovação manual antes de liberar pagamento?
