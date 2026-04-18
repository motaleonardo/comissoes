# Regras de Extração de Dados

> Status: rascunho inicial para definição com o usuário.

## Objetivo

Definir quais dados serão extraídos do SQL Server / BDN para compor a base normalizada de apuração de comissões.

## Parâmetros de Entrada

- Período de apuração: a definir se mensal fixo ou intervalo configurável.
- Filial: a definir se obrigatório, múltipla seleção ou todas.
- Classificação de venda: a definir se filtra antes ou depois da extração.

## Schema Normalizado Proposto

Cada linha deve representar a menor unidade apurável da comissão. A granularidade ainda precisa ser confirmada: venda, nota fiscal, item da nota ou chassi.

Colunas candidatas:

- `periodo`
- `filial`
- `vendedor_id`
- `vendedor_nome`
- `cliente_codigo`
- `cliente_nome`
- `documento`
- `data_emissao`
- `classificacao_venda`
- `produto_codigo`
- `produto_descricao`
- `chassi`
- `valor_venda`
- `valor_custo`
- `margem_bruta`
- `percentual_margem`
- `meta_margem`
- `base_calculo`

## Tabelas BDN Confirmadas

Estas já são usadas na validação de pagamento:

- `bdnIncentivos`
- `bdnFaturamento`
- `bdnContasReceber`

## Levantamento Inicial do Catálogo SQL Server

Inspeção somente-leitura realizada no catálogo do SQL Server em 17/04/2026 encontrou views candidatas no schema `dbo`.

### Fontes Candidatas para Apuração

| View | Uso provável | Campos relevantes observados |
|---|---|---|
| `bdnFaturamento` | Base geral de vendas/faturamento | Filial, Vendedor Código/Nome, Cliente Código/Nome, Nota Fiscal Número/Série, Data de Emissão, Venda/Devolução, Produto, Centro de Custo, Pedido de Venda Número, Quantidade Total, Valor Total, Valor Venda Líquida, Valor Margem, TES |
| `bdnComissionamentoVeiculos` | Possível base já consolidada de comissão de veículos | Mês/Ano, Filial, Vendedor Código/Nome, Nota Fiscal Número/Série, Cliente, Chassi, Veículo, Valor de Venda, Margem Recebida, Valor Comissão, aprovações |
| `bdnFaturamentoJohnDeere` | Possível recorte JD | Tipo, Cliente, Nota Fiscal, Centro de Custo, Produto, Valor Total, Devolução Total, Filial, Vendedor |
| `bdnFaturamentoComplementoValores` | Complementos/ajustes de valor | Filial, NF, Produto, Cliente, Valor Total, Valor Custo, Valor Desconto, Valor Venda Líquida, Valor Margem, Vendedor Código |
| `bdnFaturamentoInterno` | Faturamento interno/serviços | Filial, NF, Data de Emissão, Valor Total, Vendedor Código, Serviço Tipo |
| `bdnDevolucao` | Devoluções gerais | Filial, Vendedor, Cliente, NF, NF de origem, Venda/Devolução, Produto, Valor Devolução Total, Valor Venda Líquida, Valor Margem |
| `bdnDevolucaoMaquinas` | Devoluções de máquinas | Filial, NF, NF origem, Cliente, Vendedor Código, Data de Emissão, Valor Total, Valor Custo, Valor Venda Líquida, Valor Margem, Chassi |
| `bdnDevolucaoPecas` | Devoluções de peças | Filial, NF, Cliente, Data de Emissão, Vendedor Código, Valor Total, Valor Custo, Valor Venda Líquida, Valor Margem, Produto |
| `bdnVendedor` | Cadastro de vendedores | Vendedor Código, Nome, Documento, Centro de Custo, Código Função, Email, Código Gerente |
| `bdnPedidoVenda` | Relação pedido-venda | Filial, Pedido de Venda Número, Nota Fiscal Número, Cliente, Vendedor Código, Condição de Pagamento, Data de Emissão |
| `bdnOrcamento` / `bdnOrcamentoVenda` | Origem comercial e possível vínculo orçamento/NF | Filial, Orçamento, Cliente, Vendedor, Nota Fiscal, Chassi, Status, Valor Total |
| `bdnOportunidadesDeNegocio` | Ciclo comercial de veículos/máquinas | Filial, Oportunidade, Vendedor, Pedido, Chassi, Cliente, Nota Fiscal, valores de negociação |

### Fontes Confirmadas para Validação de Pagamento

| View | Uso |
|---|---|
| `bdnIncentivos` | Localizar NF de incentivo a partir do chassi |
| `bdnFaturamento` | Confirmar documento, cliente e data de emissão |
| `bdnContasReceber` | Verificar saldo pendente por título, cliente e tipo de título |

## Tabelas/Views BDN a Confirmar com o Usuário

- Tabela origem de vendas/notas por vendedor.
- Tabela de metas por vendedor/filial/classificação.
- Tabela de vendedores e vínculos com filial/cargo.
- Tabela de produtos/classificações, se a classificação não vier diretamente da venda.

## Hipótese Inicial de Extração

1. Usar `bdnFaturamentoMaquinas` como fonte de faturamentos.
2. Usar `bdnDevolucaoMaquinas` como fonte de devoluções.
3. Unificar as duas origens em uma tabela única com a coluna `Tipo`, usando `Faturamento` ou `Devolução`.
4. Filtrar ambas pelo mesmo período de apuração: dia 16 do mês base até dia 15 do mês seguinte.
5. Fazer left joins de enriquecimento com `bdnIncentivos`, `bdnVeiculos`, `bdnVendedor`, `bdnCliente` e `datawarehouse.dbo.bdnOrganizacaoCentroDeCusto`.
6. Manter `bdnContasReceber`, `bdnFaturamento` e `bdnIncentivos` também como camada de elegibilidade para pagamento.

## Regra Confirmada de Período

O usuário seleciona o mês base da comissão. A extração sempre usa:

- Data inicial: dia 16 do mês base.
- Data final: dia 15 do mês seguinte.

Exemplo:

- Comissão de Março/2026
- Período: 16/03/2026 até 15/04/2026

## Tabela Única de Máquinas

Fontes:

- `bdnFaturamentoMaquinas`, com `Tipo = Faturamento`
- `bdnDevolucaoMaquinas`, com `Tipo = Devolução`

Campos finais:

- `Tipo`
- `Filial`
- `Data de Emissão`
- `Nro Documento`
- `Modelo`
- `Nro Chassi`
- `Nome do Cliente`
- `CEN`
- `Classificação Venda`
- `Receita Bruta`
- `% Comissão Fat.`
- `Valor Comissão Fat.`
- `CMV`
- `Margem R$`
- `% Margem Direta`
- `Valor Incentivo`
- `Receita Bruta + Incentivos R$`
- `Margem + Incentivos R$`
- `Meta de Margem`
- `% Margem Bruta`
- `% Comissão Margem`
- `Valor Comissão Margem`
- `Valor Comissão Total`

## Joins Confirmados

| Origem | Destino | Chave | Campos trazidos |
|---|---|---|---|
| Faturamento/Devolução | `bdnIncentivos` | `Chassi` | Soma de `Valor Incentivo` por chassi |
| Faturamento/Devolução | `bdnVeiculos` | `Chassi` | `Veículo Modelo` |
| Faturamento/Devolução | `bdnVendedor` | `Vendedor Código` | `Vendedor Nome` |
| Faturamento/Devolução | `bdnCliente` | `Cliente Código` | `Cliente Nome` |
| Faturamento/Devolução | `datawarehouse.dbo.bdnOrganizacaoCentroDeCusto` | `Centro de Custo` = `Código C.Custo` convertido para texto | `Lojas`, `Descrição C.Custo` |

## Regras Confirmadas de Cálculo Inicial

Percentuais configuráveis por modelo entram inicialmente como zero:

- `% Comissão Fat. = 0`
- `Meta de Margem = 0`
- `% Comissão Margem = 0`

Posteriormente esses valores serão carregados por uma tabela de configuração alimentada por Excel.

Para faturamento:

- `Receita Bruta = Valor Total`
- `CMV = Valor Custo`
- `Margem R$ = Valor Total - Valor Custo - Valor Impostos`
- `Receita Líquida = Valor Venda Líquida`
- `% Margem Direta = Margem R$ / Receita Líquida`
- `Valor Incentivo = soma de bdnIncentivos.Valor Incentivo por chassi`
- `Receita Bruta + Incentivos R$ = Receita Bruta + Valor Incentivo`
- `Margem + Incentivos R$ = Margem R$ + Valor Incentivo`
- `% Margem Bruta = Margem + Incentivos R$ / Receita Líquida`

## Regra Confirmada de Incentivos

Os incentivos não devem ser salvos apenas dentro da tabela principal. Devem existir em uma tabela separada para auditoria e persistência externa.

Origem:

- `bdnIncentivos`

Campos usados:

- `Chassi`
- `Nota Fiscal Número`
- `Valor Incentivo`

Regra:

1. Agrupar por `Chassi` e `Nota Fiscal Número`.
2. Somar `Valor Incentivo` para cada par chassi/título.
3. Usar `bdnIncentivos.[Nota Fiscal Número]` como título de incentivo.
4. Consultar esse título em `bdnContasReceber.[Título Número]`.
5. Somar `bdnContasReceber.[Valor Saldo]` por título.
6. Se um chassi possuir mais de um título, somar os saldos de todos os títulos.
7. Se o saldo total dos títulos for zero, status do incentivo fica `APTO`.
8. Se o saldo total for maior que zero, status do incentivo fica `NÃO APTO` e deve trazer o saldo.
9. Se o título não for encontrado em `bdnContasReceber`, status fica `VERIFICAR`.

Tabela separada prevista para persistência:

- `commission_incentive_titles`

Colunas:

- `Nro Chassi`
- `Título Incentivo`
- `Valor Incentivo`
- `Saldo Incentivo`
- `Status Incentivo`
- `Detalhe Incentivo`

Persistência futura:

- Postgres próprio via variável `POSTGRES_URL`.
- A tabela principal continua trazendo somente o valor agregado de incentivo por chassi.
- A auditoria dos títulos fica fora da tabela principal.

Para devolução:

- `Receita Bruta = Valor Total`
- `CMV = Valor Custo`
- `Margem R$ = (Valor Total * -1) - Valor Custo - Valor Impostos`
- `Receita Líquida = (Valor Total * -1) - Valor Impostos`
- `% Margem Direta = Margem R$ / Receita Líquida`
- `Valor Incentivo = soma de bdnIncentivos.Valor Incentivo por chassi`
- `Receita Bruta + Incentivos R$ = Receita Bruta + Valor Incentivo`
- `Margem + Incentivos R$ = Margem R$ + Valor Incentivo`
- `% Margem Bruta = Margem + Incentivos R$ / Receita Líquida`

## Perguntas Abertas

- A comissão é apurada por emissão, faturamento, entrega ou recebimento?
- Notas canceladas/devolvidas entram como estorno no mesmo período ou no período do evento?
- O vendedor vem direto da nota, do pedido ou de uma tabela de rateio?
- Há vendas com mais de um vendedor?
- A filial usada na comissão é filial de emissão, faturamento, estoque ou vendedor?
- O chassi é obrigatório apenas para `Maquinas JD - Novos`?
