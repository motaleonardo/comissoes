# Venda Direta (CVD JD) e Comissão de Consórcio — Design

> Data: 2026-09-22
> Status: aprovado em brainstorming, pendente de plano de implementação.

Duas entregas independentes, na ordem abaixo:

- **Parte A — Venda Direta (CVD JD):** passa a apurar o produto `CVD JD`, hoje excluído da extração.
- **Parte B — Consórcio:** nova tela de apuração alimentada por upload dos relatórios do sistema de consórcio.

---

# Parte A — Venda Direta (CVD JD)

## Contexto

A extração de máquinas exclui de propósito todo produto cujo `[Produto Código]` contém `CVD`. O filtro aparece em seis pontos (faturamento e devolução) de três consultas em `commission_tool/data/sources/sqlserver.py`:

- `extract_machine_commission_base` (linhas 513 e 545)
- `extract_machine_source_audit` (linhas 313 e 327)
- `extract_machine_incentive_audit` (linhas 362 e 374)

Levantamento feito no SQL Server em 21/09/2026 (`bdnFaturamentoMaquinas`; `bdnDevolucaoMaquinas` não tem nenhuma linha CVD):

| Código | Linhas | Chassis | Valor total | Período |
|---|---:|---:|---:|---|
| CVD INCENTIVO | 3.739 | 0 | R$ 147,1 mi | 2015 a fev/2024 |
| **CVD JD** | **670** | **205** | **R$ 50,6 mi** | 2015 a set/2026 |
| CVD JDT | 254 | 0 | R$ 4,4 mi | até jul/2026 |
| CVD CONSORICIO | 247 | 0 | R$ 16,0 mi | até set/2026 |
| CVD MAPFRE | 113 | 0 | R$ 0,5 mi | até 2023 |
| CVD SOMPO 02 | 65 | 0 | R$ 0,1 mi | até jul/2026 |
| CVD SOMPO | 57 | 0 | R$ 0,7 mi | até jul/2026 |
| CVD | 16 | 0 | R$ 24,8 mi | até 2023 |

Desde o corte de 01/12/2025 são 60 linhas de `CVD JD` e R$ 6,6 mi. Dessas, 42 (R$ 5,4 mi) não têm chassi em `bdnVeiculos`. Nenhum chassi de `CVD JD` aparece também em faturamento não-CVD, ou seja, não há duplicidade com a apuração atual.

## Decisões

1. Apenas `CVD JD` passa a ser apurado. A comparação é por **igualdade exata**, porque `LIKE 'CVD JD%'` capturaria `CVD JDT`. Os demais códigos CVD, inclusive `CVD CONSORICIO`, continuam fora.
2. `CVD JD` tem classificação **Venda Direta**, que já existe no relatório por CEN e no lançamento manual.
3. Comissão do CEN: **6% sobre a Receita Bruta** (`[Valor Total]` da nota). Sem comissão de margem e sem meta.
4. Comissão do gerente: **33% sobre a comissão do CEN**, pela regra atual de `comissao_gerente_vendedor`. Nada muda em `apply_manager_commission_rules`.
5. A coluna `Produto Código` passa a ser extraída, exibida, persistida e exportada para todas as linhas, não só para o CVD.
6. Elegibilidade: nada muda. A V1 (incentivo por chassi) só vale para `Maquinas JD - Novos`, e a V2 (título quitado) continua valendo, o que garante que o valor tenha sido recebido antes do pagamento.

## Lacuna corrigida nesta entrega: duplicidade sem chassi

A trava de "já pago" compara o par **chassi + documento**, e as três consultas de histórico descartam linhas sem chassi (`WHERE TRIM(nro_chassi) <> ''`, em `postgres.py:804`, `:831` e `:855`). Como 70% das linhas de `CVD JD` não têm chassi, uma nota já paga voltaria como "Novo para apuração" numa extração posterior.

Correção: a chave de confronto passa a ser condicional.

- Linha **com chassi**: chave `chassi || documento`, como hoje.
- Linha **sem chassi**: chave `documento`, e o confronto passa a considerar também os pagamentos e exclusões sem chassi.

Isso vale para `comissoespagas` e `comissoesexcluidas`, e o comportamento das linhas com chassi permanece idêntico.

## Alterações

| Arquivo | Alteração |
|---|---|
| `data/sources/sqlserver.py` | Filtro vira `(produto IS NULL OR produto NOT LIKE '%CVD%' OR LTRIM(RTRIM(produto)) = 'CVD JD')` nos seis pontos; `[Produto Código]` entra na CTE `base` e no SELECT final; o CASE da classificação ganha `WHEN produto = 'CVD JD' THEN 'Venda Direta'` como primeira condição |
| `core/apuracao.py` | `VENDA_DIRETA_RATE = 6.0`; máscara de Venda Direta no mesmo padrão de Implemento e Usados, zerando `% Comissão Margem` e `Meta de Margem`; confronto de histórico condicional ao chassi |
| `data/sources/postgres.py` | `produto_codigo` em `comissoespagas` e `comissoesexcluidas` (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`), entrada em `PAID_COLUMN_MAP`, e os três resumos passam a expor também as linhas sem chassi |
| `core/reports.py` | `produto_codigo` → `Produto Código` no `ANALYTIC_REPORT_COLUMN_MAP` |
| `ui/app.py` | Coluna `Produto Código` na tabela da apuração |
| `docs/regras_calculo.md` | Registro da regra de Venda Direta |

## Testes

- A consulta inclui `CVD JD` e continua excluindo `CVD`, `CVD JDT`, `CVD INCENTIVO` e `CVD CONSORICIO`.
- Linha `CVD JD` recebe classificação Venda Direta mesmo sem registro em `bdnVeiculos`.
- Venda Direta calcula 6% sobre a receita bruta, com comissão de margem zero mesmo quando há meta cadastrada para o modelo.
- Gerente recebe 33% sobre a comissão do CEN da linha de Venda Direta.
- Confronto de histórico: linha sem chassi cujo documento já foi pago fica bloqueada; linha sem chassi de documento novo fica apta; linha com chassi mantém o comportamento atual.

---

# Parte B — Comissão de Consórcio

## Contexto

Hoje a apuração é manual, numa planilha (`Relatorio Cotas Consórcio Agosto 26.xlsx`): os CSV exportados do sistema de consórcio, um por PDV, são colados numa aba `Base`, as colunas `Filial` e `Gestor` são preenchidas à mão, três colunas de fórmula calculam os valores e duas tabelas dinâmicas consolidam o resultado.

Entrada: um CSV por loja, UTF-8 com BOM, separador `;`, decimal com vírgula, datas `dd/mm/aaaa`. Cada linha é uma cota (`CD_Grupo` + `CD_Cota` + `Versao`) numa parcela da comissão (`Parcela`, no formato `00X / 003`).

Colunas do arquivo: `CD_Pedido`, `NM_Produto`, `CD_Comissionado_PDV`, `NM_Comissionado_PDV`, `CD_Comissionado_Vendedor_Atual` **ou** `CD_Comissionado_Vendedor_Original` (varia por loja), o nome correspondente, `Consorciado`, `CD_Grupo`, `CD_Cota`, `Versao`, `ST_Contemplacao`, `CD_Periodo`, `DT_Alocacao`, `DT_Aprovacao`, `VL_Bem`, `Parcela`, `PE_Comissao`, `VL_Comissao`.

Regra atual, com base em `VL_Bem` (o `VL_Comissao` do arquivo é a comissão que a concessionária recebe e não é usado no cálculo):

| Parcela | CEN | Gestor da loja | Coordenador |
|---|---|---|---|
| 001 / 003 | 0,84% | – | – |
| 002 / 003 | – | 0,21% | 0,15% |
| 003 / 003 | – | – | – |

Resultado de agosto/2026, que serve de critério de aceite: **CENs R$ 112.560,00**, **gestores R$ 8.456,56**, **coordenador R$ 6.040,40** (Djalma Rodrigues de Paula Junior).

## Arquitetura

Fluxo separado do de máquinas, com tela própria ("Apuração Consórcio" no menu lateral) e tabelas próprias. O resultado aparece nos relatórios existentes em colunas dedicadas — a opção híbrida escolhida no brainstorming.

| Módulo novo | Responsabilidade |
|---|---|
| `io/consorcio_csv.py` | Ler e normalizar o CSV; aceitar os dois layouts de vendedor; recusar arquivo com coluna obrigatória faltando |
| `core/consorcio.py` | Funções puras: de-paras, status e bloqueios, cálculo dos três valores, tabelas da premiação |
| `data/sources/postgres_consorcio.py` | As cinco tabelas do consórcio, com leitura e gravação |
| `ui/consorcio.py` | Tela de apuração e seções de Configurações do consórcio |

Os módulos existentes mudam pouco: `ui/app.py` (item de menu e chamada das seções), `core/reports.py` (colunas novas e quadro do coordenador) e `core/report_email.py` (aba extra e e-mail do coordenador).

## Modelo de dados (Postgres)

Todas as tabelas de configuração seguem o padrão atual, com `ativo BOOLEAN` e `updated_at`, e uma nova versão desativa a anterior.

**`comissao_consorcio_pdv`** — PDV do consórcio para filial.

| Campo | Observação |
|---|---|
| `cd_pdv` | Normalizado sem zeros à esquerda: `002001` e `2001` são o mesmo PDV |
| `nm_pdv` | Nome como vem do arquivo, apenas informativo |
| `filial` | Precisa bater com a filial usada em máquinas |

Carga inicial a partir de `backups/de_para_comissao_consorcio.xlsx` (aba `pdv_consorcio`, 9 linhas). O PDV `002001`, que aparece dentro do arquivo de Goiânia, não está nessa carga e entra como pendente.

**`comissao_consorcio_vendedor`** — vendedor do consórcio para CEN do ERP.

| Campo | Observação |
|---|---|
| `cd_vendedor_consorcio` | Código do sistema de consórcio (ex.: `002197`) |
| `nm_vendedor_consorcio` | Nome como vem do arquivo |
| `cod_vendedor` | Código do ERP (ex.: `00188`). **Nasce em branco** |

Cada upload cadastra automaticamente os vendedores ainda desconhecidos com `cod_vendedor` em branco, para preenchimento em Configurações. O sistema nunca preenche esse campo sozinho.

**`comissao_consorcio_regras`** — percentual por parcela.

| Campo | Agosto/2026 |
|---|---|
| `parcela` | `001 / 003`, `002 / 003`, `003 / 003` |
| `percentual_cen` | 0,84 / 0 / 0 |
| `percentual_gestor` | 0 / 0,21 / 0 |
| `percentual_coordenador` | 0 / 0,15 / 0 |

**`comissao_consorcio_coordenador`** — `nome` e `email` do coordenador (Djalma Rodrigues de Paula Junior).

**`comissoes_consorcio_pagas`** — histórico de pagamento, uma linha por cota e parcela.

Campos: `competencia_ano`, `competencia_mes`, `mes_ano_comissao`, `arquivo_origem`, `paid_at`, `cd_pedido`, `cd_grupo`, `cd_cota`, `versao`, `parcela`, `st_contemplacao`, `dt_alocacao`, `dt_aprovacao`, `consorciado`, `vl_bem`, `cd_pdv`, `filial`, `cd_vendedor_consorcio`, `nm_vendedor_consorcio`, `cod_vendedor`, `cen`, `gerente`, `coordenador`, `percentual_cen`, `valor_comissao_cen`, `percentual_gestor`, `valor_comissao_gestor`, `percentual_coordenador`, `valor_comissao_coordenador`.

Os percentuais são gravados junto com o valor, então uma mudança de regra não altera o histórico. A tabela tem **restrição de unicidade em (`cd_grupo`, `cd_cota`, `versao`, `parcela`)** como última barreira contra pagamento em duplicidade.

## Enriquecimento e cálculo

Para cada linha do arquivo:

1. `cd_pdv` → `filial`, por `comissao_consorcio_pdv`.
2. `cd_vendedor_consorcio` → `cod_vendedor`, por `comissao_consorcio_vendedor`.
3. `cod_vendedor` → `cen`, `gerente`, e os e-mails, por `comissao_gerente_vendedor` (a mesma fonte usada em máquinas, o que mantém uma única verdade sobre quem é gestor de quem).
4. `parcela` → percentuais, por `comissao_consorcio_regras`.
5. Valores: `valor = vl_bem * percentual / 100`, um para cada beneficiário.

O gestor do consórcio recebe o percentual da regra sobre o valor do bem. A regra de 33% sobre a comissão do CEN **não se aplica** ao consórcio.

## Status e travas

Só linhas **APTAS** podem ser selecionadas para pagamento.

| Situação | Status |
|---|---|
| Tudo resolvido e a parcela gera valor | ✅ APTO |
| Parcela sem comissão para ninguém (`003 / 003`) | Sem comissão (exibida, com valor zero) |
| PDV sem de-para | ❌ Bloqueado: PDV sem de-para |
| Vendedor do consórcio sem `cod_vendedor` | ❌ Bloqueado: vendedor sem de-para |
| `cod_vendedor` sem relação ativa em `comissao_gerente_vendedor`, quando a parcela paga gestor | ❌ Bloqueado: sem gerente |
| Cota e parcela já pagas | ❌ Bloqueado: já pago em MM/AAAA |
| Cota e parcela repetidas no mesmo upload | ❌ Bloqueado: duplicado no upload |
| Parcela sem regra cadastrada, ou fora do padrão `00X / 003` | ❌ Bloqueado: parcela sem regra |
| `VL_Bem` ausente, não numérico ou ≤ 0 | ❌ Bloqueado: valor do bem inválido |

Arquivo com coluna obrigatória faltando é recusado inteiro, com a lista do que falta. Nenhum bloqueio é resolvido por suposição do sistema: a saída é sempre completar o de-para em Configurações e subir o arquivo de novo.

## Telas

**Apuração Consórcio** (nova, no menu lateral, abaixo de "Apurações"):

1. Escolha da competência (mês e ano).
2. Upload de vários CSV, com o retorno de linhas lidas e layout aceito por arquivo.
3. Tabela de conferência com status, os três valores por linha, totais no topo e o aviso de pendências de de-para.
4. Seleção, com as linhas aptas já marcadas e as bloqueadas não selecionáveis.
5. Registro do pagamento, com revisão e confirmação, gravando em `comissoes_consorcio_pagas`.
6. Exportação em Excel com as abas `Base` e `Premiação`, espelhando a planilha atual.

**Configurações**, quatro seções novas, no padrão das existentes (upload de Excel mais editor de tabela):

- Consórcio: PDV x Filial
- Consórcio: Vendedor x CEN — pendentes no topo e uma coluna somente leitura "Sugestão (nome igual na relação CEN x Gerente)"
- Consórcio: regras por parcela
- Consórcio: coordenador

**Relatórios:**

- Relatório por CEN: nova coluna **Consórcio**, somada ao total.
- Relatório por gerente: nova coluna **Gestor Consórcio**, fora da regra de 33%.
- Quadro do coordenador de consórcio, no mesmo padrão do coordenador de implementos.

**E-mails:** o CEN e o gestor recebem o analítico de consórcio como aba adicional do e-mail que já recebem; o coordenador recebe um e-mail próprio, como o de implementos.

## Testes

- **Parser:** os dois layouts de vendedor; BOM; decimal com vírgula (`206994,15`); data `dd/mm/aaaa`; arquivo com coluna faltando é recusado com a lista.
- **Cálculo:** os percentuais de cada parcela; parcela sem regra bloqueia em vez de assumir zero; `VL_Bem` inválido bloqueia.
- **Travas:** PDV sem de-para; vendedor sem de-para; sem gerente; já pago; duplicado no upload; unicidade no banco.
- **De-paras:** vendedor novo é cadastrado com `cod_vendedor` em branco; a sugestão por nome não preenche o campo.
- **Relatórios:** coluna Consórcio no relatório por CEN; Gestor Consórcio fora dos 33%; quadro do coordenador.
- **Aceite de agosto:** os CSV contêm nomes de consorciados e **não vão para o repositório**. Os testes versionados usam dados sintéticos; a conferência dos R$ 112.560,00 / R$ 8.456,56 / R$ 6.040,40 roda como script local sobre a pasta do usuário.

## Fora de escopo

- Demais códigos CVD, inclusive `CVD CONSORICIO` no faturamento de máquinas, que não se confunde com esta apuração.
- Uso da coluna `paga_comissao_cen` criada por `scripts/link_cen_aaetjv_to_manager.py`, que segue pendente.
- Percentual do gerente por relação no relatório por gerente de máquinas, hoje fixo em 33% em `core/reports.py`.

## Ordem de entrega

1. Parte A — Venda Direta (CVD JD).
2. Consórcio: de-paras, regras e Configurações.
3. Consórcio: tela de apuração e gravação.
4. Consórcio: relatórios e e-mails.
