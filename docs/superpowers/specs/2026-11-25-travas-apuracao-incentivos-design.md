# Travas de cadastro, incentivos sem nota e CVD JD na apuração de máquinas

Data: 25/11/2026 — tamanho: mudança pequena (design curto)

## Por que

- O padrão de 1% para modelo sem regra já pagou errado (Outubro/2026: máquinas novas a 1% em vez de
  0,45–0,60% e gerente zerado). Falta de cadastro tem que parar a apuração, não ser preenchida por suposição.
- CEN sem gerente hoje passa com comissão de gerente zerada, sem aviso forte.
- Incentivos sem `Nota Fiscal Número` são incentivos reais do chassi; a nota só importa para a validação do
  Contas a Receber.
- `CVD JD` (Venda Direta) não depende de quitação do cliente para ser paga.

## O que muda

### 1. Incentivos sem nota entram na extração
- Em `extract_machine_commission_base` (`sqlserver.py`), o CTE `incentivo_titulos` deixa de exigir
  `Nota Fiscal Número` preenchida; continua exigindo chassi.
- Efeito em dinheiro, aceito pelo usuário: `Valor Incentivo` entra no `% Margem Bruta`, então alguns
  chassis podem passar a bater a meta de margem.
- A tabela de títulos de incentivo (`extract_incentive_titles_by_chassi` / resumo) **não muda**: ela é por
  título, para o Contas a Receber.
- Correção junto: `find_incentive_invoice_by_chassi` hoje pode devolver o texto `"None"` quando a primeira
  linha do chassi não tem nota. Passa a considerar só linhas com nota. Chassi que só tem incentivo sem nota
  → V1 `NÃO ENCONTRADO`, detalhe "Incentivo sem nota fiscal em bdnIncentivos" (status geral VERIFICAR).
  *Decisão minha.*

### 2. CVD JD dispensado da validação do Contas a Receber
- Em `run_eligibility_validation`, linha com `Produto Código` exatamente `CVD JD` não consulta V1 nem V2:
  ambos `N/A`, detalhe "Venda Direta (CVD JD) dispensada da validação", Status Geral **APTO**.
- O critério é o `Produto Código`, não a classificação.

### 3 e 4. Trava por regra de modelo e por gerente do CEN
- **Fim do padrão de 1%:** sai o toggle "Aplicar padrão de 1%..." e a chamada a
  `apply_frontend_default_fat_commission` no fluxo de Apurações.
- **Linhas conferidas:** só as que ainda podem ser pagas — `machine_df` (já sem pagas e excluídas) depois
  do filtro de emissões anteriores a 01/12/2025 (se o filtro estiver ligado). Se o filtro for desligado e
  aparecerem linhas novas, a conferência roda de novo.
- **Pendência de modelo:** linha com `Regra Comissão Fat. Encontrada = False`. Implemento, Usados e Venda
  Direta nunca caem aqui (percentual fixo). Falta de regra de **margem não trava** (margem segue zero).
  Modelo vazio aparece como "(sem modelo)" com o chassi de exemplo.
- **Pendência de gerente:** linha com `Regra Gerente Encontrada = False`. Agrupada por `Cod Vendedor` + CEN
  + Filial. Linha sem código de vendedor aparece como "(sem código)".
- A lógica de identificar pendências fica no `core` (função pura, testada), devolvendo duas tabelas:
  modelos (Modelo, Classificação, Qtd linhas, Chassi exemplo) e CENs (Cod Vendedor, CEN, Filial, Qtd linhas).

### Comportamento na tela (abordagem A)
- A extração roda e guarda os dados normalmente.
- Antes de mostrar a tabela de apuração, roda a conferência. Havendo pendência:
  - abre um popup (`st.dialog`) **uma vez por extração/reaplicação** com as duas listas completas e o
    botão **"Ir para Configurações"**;
  - a tela de Apurações mostra um aviso fixo com as mesmas listas, botão para reabrir o popup, o botão
    "Ir para Configurações" e o botão **"Reaplicar regras"**;
  - tabela, validação, exclusão e pagamento ficam ocultos (a função retorna antes deles).
- **"Ir para Configurações":** a navegação da barra lateral ganha uma `key` e o botão a troca via callback.
- **"Reaplicar regras":** relê do Postgres as regras de faturamento, margem, relação CEN x Gerente e o
  histórico de pagos/excluídos, e refaz `prepare_machine_apuracao` sobre o `machine_raw_df` já extraído —
  sem nova consulta ao SQL Server.
- **Configurações:** no topo, um quadro "Pendências da última apuração" com os modelos e códigos de CEN que
  faltam (vindo do `session_state`), só quando houver pendência.

## Abordagens consideradas
- **A — conferir na hora de mostrar + reaplicar regras (escolhida):** cobre o caso do filtro de emissões
  desligado e evita extrair de novo a cada correção.
- B — conferir só no clique de extrair e descartar os dados: perde linhas que surgem ao desligar o filtro e
  obriga nova extração.
- C — só avisar e desabilitar pagar: não interrompe a apuração como pedido.

## Fora de escopo
- Corrigir os registros antigos de `comissoespagas` com % ×100.
- Mudar a apuração de consórcio, relatórios ou lançamento manual.
- Travar por falta de regra de margem.

## Deixado para quem implementa
- Texto exato das mensagens e layout do popup/aviso.
- Se `apply_frontend_default_fat_commission` é removida ou só deixa de ser chamada (preferir remover se
  nada mais usar, junto com seus testes).
