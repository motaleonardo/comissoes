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

## Uso

1. Abra http://localhost:8501 no navegador
2. Na barra lateral, configure a conexão SQL Server (servidor, banco, usuário/senha)
3. Clique em **Testar Conexão**
4. Faça upload do arquivo Excel de comissões (aba: Analitico CEN)
5. Clique em **Executar Validação Completa**
6. Analise os resultados e exporte conforme necessário

## Tabelas SQL Server esperadas

| Tabela              | Campos utilizados                                      |
|---------------------|--------------------------------------------------------|
| `bdnIncentivos`     | `[Chassi]`, `[Nota Fiscal Número]`                    |
| `bdnFaturamento`    | `[Nota Fiscal Número]`, `[Data de Emissão]`, `[Cliente Código]` |
| `bdnContasReceber`  | `[Título Número]`, `[Cliente Código]`, `[Valor Saldo]`, `[Tipo Título]`, `[Data de Emissão]` |

## Regras de negócio

### Validação 1 — Incentivo
- Busca chassi em `bdnIncentivos` → captura NF do incentivo
- Soma `[Valor Saldo]` em `bdnContasReceber` para esse título
- **= 0** → APTO | **≠ 0** → NÃO APTO (traz saldo e data de emissão)

### Validação 2 — Pagamento do Cliente
- Busca `Nro Documento` em `bdnFaturamento` (confirma data de emissão)
- Captura `[Cliente Código]`
- Soma `[Valor Saldo]` em `bdnContasReceber` (cliente + título)
- **= 0** → APTO
- **≠ 0 + tipo exclusivamente BL** → APTO (regra especial)
- **≠ 0 + outros tipos** → NÃO APTO (traz saldo e tipos separados por `/`)
