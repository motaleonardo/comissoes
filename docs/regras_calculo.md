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

## Venda Direta (CVD JD)

Vigente a partir de 09/2026, implementado em `commission_tool/core/apuracao.py`.

- Entram apenas as linhas de `bdnFaturamentoMaquinas` cujo `[Produto Código]` é exatamente `CVD JD`. Os demais códigos com `CVD` continuam fora da apuração.
- Classificação: `Venda Direta`, atribuída pelo código do produto, e não pelo cadastro do veículo (a maioria dessas linhas não tem chassi).
- Comissão do CEN: 6% sobre a Receita Bruta. Sem comissão de margem e sem meta.
- Comissão do gerente: 33% sobre a comissão do CEN, pela relação em `comissao_gerente_vendedor`.
- Confronto com o histórico: linha com chassi usa chassi + documento; linha sem chassi usa apenas o documento.
- Limitação conhecida: quando uma nota sem chassi tem mais de uma linha, a trava é por documento. Todas as linhas dessa nota devem ser pagas na mesma apuração; se apenas parte for paga, o restante fica bloqueado como "Já tratado neste documento" e precisa de correção manual.

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
