# 📐 Commission Tool — Project Spec
> Última atualização: Abril 2026

## Atualização de Implementação — Abril 2026

- A aplicação Streamlit da raiz (`app.py`) agora é apenas um entrypoint.
- A validação de elegibilidade foi migrada para `commission_tool/core/eligibility.py`.
- As consultas SQL Server / BDN foram isoladas em `commission_tool/data/sources/sqlserver.py`.
- A leitura/exportação Excel temporária foi isolada em `commission_tool/io/excel.py`.
- Foram criados documentos separados para `regras_extracao.md`, `regras_calculo.md` e `regras_validacao_pagamento.md`.
- Foram adicionados testes unitários iniciais para as regras de validação.

---

## 🧭 Visão Geral

**O que este projeto faz:**  
Ferramenta de apuração e validação de comissões de venda que executa um pipeline em três estágios:

```
[1] Extração  →  [2] Cálculo  →  [3] Validação de Elegibilidade
```

1. **Extração** — coleta dados de vendas, faturamento e metas diretamente do SQL Server (BDN)
2. **Cálculo** — aplica as regras de comissão sobre os dados extraídos
3. **Validação** — verifica se os pré-requisitos estão satisfeitos (incentivo recebido + cliente quitado) antes de liberar o pagamento

**Estado atual do projeto:**  
- Estágio 3 (validação) está funcional  
- Estágios 1 e 2 (extração e cálculo) estão a construir  
- As regras de cálculo serão documentadas em `regras_calculo.md`

---

## 🏗️ Arquitetura Alvo

```
commission_tool/
├── core/
│   ├── calculator.py        # Lógica pura de cálculo — sem I/O, 100% testável
│   ├── rules.py             # Regras de negócio (ver regras_calculo.md)
│   └── models.py            # Dataclasses / modelos de dados
├── data/
│   ├── sources/
│   │   ├── sqlserver.py     # Conector SQL Server — extração e validação
│   │   └── base.py          # Interface abstrata para novas fontes
│   └── pipeline.py          # Orquestrador: extrai, normaliza, une
├── ui/
│   └── app.py               # Interface Streamlit
├── tests/
│   └── test_calculator.py   # Testes unitários da lógica de cálculo
├── regras_calculo.md        # Fonte de verdade das regras de cálculo
├── config.py                # Configurações e parâmetros
└── main.py                  # Entrypoint CLI
```

---

## 🧠 Stack

| Camada | Tecnologia |
|---|---|
| Linguagem | Python 3.9+ |
| Banco de dados | SQL Server (pyodbc) |
| Interface | Streamlit |
| Processamento | pandas |
| Testes | pytest |

---

## 🔌 Fontes de Dados

### Fonte única — SQL Server / BDN (SIGAOFC)

Toda extração é feita diretamente do SQL Server. Não há leitura de planilhas Excel como entrada.

**Servidor:** 172.17.27.152 | **Base:** SIGAOFC | **Porta:** 1433  
**Autenticação:** Windows (Trusted Connection) ou SQL  
**Driver:** ODBC Driver 17/18 for SQL Server

### Tabelas de Validação (estágio 3)

| Tabela | Campos relevantes | Uso |
|--------|-------------------|-----|
| `bdnIncentivos` | `[Chassi]`, `[Nota Fiscal Número]` | Mapeia chassis → NF de incentivo |
| `bdnFaturamento` | `[Nota Fiscal Número]`, `[Data de Emissão]`, `[Cliente Código]` | Identifica cliente e data da venda |
| `bdnContasReceber` | `[Título Número]`, `[Cliente Código]`, `[Valor Saldo]`, `[Tipo Título]`, `[Data de Emissão]` | Verifica saldo pendente por tipo de título |

### Tabelas de Extração (estágio 1)

> A definir com o usuário em `regras_calculo.md`.

---

## ⚙️ Estágio 1 — Extração

> Princípio: toda lógica de extração fica em `data/sources/sqlserver.py`. Nenhuma query SQL deve existir fora desse módulo.

- Extrai dados de vendas, metas e vendedores das tabelas BDN
- Normaliza para schema padrão (`pd.DataFrame`) antes de passar ao cálculo
- Parâmetros de entrada: período (mês/ano), filial, classificação

---

## 🧮 Estágio 2 — Cálculo

> Princípio: toda lógica de cálculo fica em `core/calculator.py`. Nenhuma query SQL ou leitura de BD deve conter lógica de comissão.

```python
# core/calculator.py
def calculate_commission(sales: pd.DataFrame, rules: CommissionRules) -> pd.DataFrame:
    """
    Recebe DataFrame normalizado de vendas e objeto de regras.
    Retorna DataFrame com colunas: vendedor_id, periodo, base_calculo,
    percentual_aplicado, comissao_bruta, ajustes, comissao_liquida.
    """
    ...
```

**Regras de cálculo:** ver `regras_calculo.md` (a ser detalhado com o usuário).

---

## ✅ Estágio 3 — Validação de Elegibilidade

> Status: **implementado** em `app.py`. A refatorar para `core/` durante a Fase 1.

Verifica se cada comissão calculada tem os pré-requisitos satisfeitos para pagamento.

### V1 — Validação de Incentivo

**Aplica-se a:** classificação `"Maquinas JD - Novos"` (única com exigência de chassi)

| Condição | Resultado |
|----------|-----------|
| Chassi vazio / nulo | N/A |
| Chassi não encontrado em `bdnIncentivos` | NÃO ENCONTRADO |
| Saldo em `bdnContasReceber` < R$0,01 | **APTO** |
| Saldo em `bdnContasReceber` ≥ R$0,01 | **NÃO APTO** — exibe saldo + data de emissão |

### V2 — Validação de Pagamento do Cliente

| Condição | Resultado |
|----------|-----------|
| Documento vazio / nulo | N/A |
| Documento não encontrado em `bdnFaturamento` | NÃO ENCONTRADO |
| Saldo em `bdnContasReceber` = 0 | **APTO** |
| Saldo ≠ 0 e **apenas** tipo `"BL"` (boleto) pendente | **APTO** — exceção de regra |
| Saldo ≠ 0 com outros tipos pendentes | **NÃO APTO** — exibe saldo + tipos |
| Data divergente entre extração e faturamento | **ATENÇÃO** — continua validando |

### Status Geral (combinado V1 + V2)

| Condição | Status |
|----------|--------|
| V1 e V2 ambas APTO ou N/A | ✅ APTO |
| Qualquer uma NÃO APTO | ❌ NÃO APTO |
| ERRO ou NÃO ENCONTRADO em qualquer validação | ⚠️ VERIFICAR |

---

## 🖥️ Interface (Streamlit)

### Tela atual (funcional)
- Conexão com SQL Server (sidebar com teste de conectividade)
- Upload de planilha Excel como entrada temporária *(a ser substituído por extração direta)*
- Execução das validações V1 + V2
- KPIs: Total / Aptos / Não Aptos / Verificar / Valores
- Filtros: Status Geral, Filial, V1 Status
- Exportação: todos os resultados ou apenas NÃO APTOS / VERIFICAR (Excel)

### Telas a construir

1. **Painel de Apuração**
   - Seletor de período (mês/ano) e filial
   - Botão "Extrair e Calcular"
   - Tabela com resultado por vendedor
   - Total consolidado

2. **Detalhe por Vendedor**
   - Breakdown: base de cálculo → regra aplicada → ajustes → líquido
   - Lista de vendas individuais que compõem o cálculo

3. **Validação de Elegibilidade**
   - Resultado das validações V1 e V2 por venda
   - Filtros e exportação

---

## 🚦 Fases de Desenvolvimento

### Fase 0 — Validação ✅ (concluída)
- [x] Validação V1 (incentivo via chassi)
- [x] Validação V2 (pagamento cliente com exceção BL)
- [x] Interface Streamlit com upload Excel, KPIs, filtros e exportação
- [x] Conexão SQL Server via pyodbc com autenticação Windows/SQL

### Fase 1 — Fundação
- [ ] Criar estrutura de pastas conforme arquitetura alvo
- [ ] Migrar código existente (`app.py`) para os módulos corretos
- [ ] Extrair queries SQL para `data/sources/sqlserver.py`
- [ ] Extrair lógica de validação para `core/` com testes unitários

### Fase 2 — Extração
- [ ] Mapear tabelas BDN necessárias para o estágio 1
- [ ] Implementar `extract()` em `sqlserver.py`
- [ ] Criar `pipeline.py` que normaliza e une os dados
- [ ] Remover dependência de Excel como entrada

### Fase 3 — Cálculo
- [ ] Finalizar `regras_calculo.md` com o usuário
- [ ] Implementar `core/calculator.py` com as regras documentadas
- [ ] Escrever testes cobrindo os casos principais

### Fase 4 — Interface Final
- [ ] Refatorar Streamlit com as telas de apuração e detalhe
- [ ] Conectar UI ao pipeline e ao calculator
- [ ] Adicionar feedback visual de erros de validação

---

## 🤖 Princípios de Desenvolvimento

```
1. Toda lógica de cálculo fica em core/calculator.py — nunca em queries ou na UI
2. Toda lógica de extração fica em data/sources/sqlserver.py — nunca na UI ou no core
3. O pipeline normaliza tudo para o schema padrão antes de qualquer cálculo
4. Nenhuma credencial hardcoded — usar variáveis de ambiente via python-dotenv
5. Funções puras são preferidas — facilita testes
6. Preserve comportamento ao refatorar — mude a estrutura, não a lógica
```

---

## 📋 Decisões em Aberto

| # | Decisão | Status |
|---|---|---|
| 1 | Quais tabelas BDN para extração (estágio 1)? | ⬜ A definir |
| 2 | Regras de cálculo de comissão (faixas, %, metas, deduções) | ⬜ A detalhar em `regras_calculo.md` |
| 3 | Período de apuração: mensal fixo ou configurável? | ⬜ A definir |
| 4 | Há múltiplos planos de comissão (por cargo / produto / classificação)? | ⬜ A definir |
| 5 | O resultado calculado precisa de aprovação formal antes do pagamento? | ⬜ A definir |
| 6 | Há necessidade de histórico / auditoria dos cálculos? | ⬜ A definir |

---

## 🗒️ Notas Técnicas

- SQL Server está na rede local (sem VPN complexa) — IP: 172.17.27.152
- Projeto interno — sem necessidade de autenticação de usuário na v1
- Variáveis de ambiente via `.env` (não commitar credenciais)
- O `app.py` atual é monolítico (873 linhas) — refatoração planejada na Fase 1
