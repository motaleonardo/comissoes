"""Investigation script: document 000046254 client code mismatch."""
import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

driver     = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
server     = os.getenv("DB_SERVER")
database   = os.getenv("DB_NAME")
trust_cert = os.getenv("DB_TRUST_SERVER_CERTIFICATE", "no")
encrypt    = os.getenv("DB_ENCRYPT", "yes")

conn_str = (
    f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
    f"Trusted_Connection=yes;TrustServerCertificate={trust_cert};Encrypt={encrypt};"
)
conn = pyodbc.connect(conn_str, timeout=10)
cursor = conn.cursor()

DOC = "000046254"

print("=" * 80)
print(f"INVESTIGATION: Document {DOC}")
print("=" * 80)

# 0. Check column names in bdnFaturamento
print("\n--- 0. Columns in bdnFaturamento ---")
cursor.execute("""
    SELECT TOP 1 * FROM bdnFaturamento
""")
cols = [desc[0] for desc in cursor.description]
for c in cols:
    print(f"  {c}")

# 1. What does bdnFaturamento say for this NF?
print(f"\n--- 1. bdnFaturamento WHERE [Nota Fiscal Número] = '{DOC}' ---")
cursor.execute("""
    SELECT TOP 5 *
    FROM bdnFaturamento
    WHERE LTRIM(RTRIM([Nota Fiscal Número])) = ?
""", (DOC,))
rows = cursor.fetchall()
cols = [desc[0] for desc in cursor.description]
if rows:
    for r in rows:
        for c, v in zip(cols, r):
            print(f"  {c}: {v}")
        print()
else:
    print("  NOT FOUND")

# 2. bdnContasReceber for client 142235 + this title
print(f"\n--- 2. bdnContasReceber WHERE [Cliente Código]='142235' AND [Título Número]='{DOC}' ---")
cursor.execute("""
    SELECT [Cliente Código], [Título Número], [Valor Saldo], [Tipo Título], [Data de Emissão]
    FROM bdnContasReceber
    WHERE LTRIM(RTRIM([Cliente Código])) = '142235'
      AND LTRIM(RTRIM([Título Número])) = ?
""", (DOC,))
rows = cursor.fetchall()
if rows:
    for r in rows:
        print(f"  Cliente: {r[0]}  |  Título: {r[1]}  |  Saldo: {r[2]}  |  Tipo: {r[3]}  |  Emissão: {r[4]}")
else:
    print("  NOT FOUND")

# 3. bdnContasReceber for client VDHZI8 + this title
print(f"\n--- 3. bdnContasReceber WHERE [Cliente Código]='VDHZI8' AND [Título Número]='{DOC}' ---")
cursor.execute("""
    SELECT [Cliente Código], [Título Número], [Valor Saldo], [Tipo Título], [Data de Emissão]
    FROM bdnContasReceber
    WHERE LTRIM(RTRIM([Cliente Código])) = 'VDHZI8'
      AND LTRIM(RTRIM([Título Número])) = ?
""", (DOC,))
rows = cursor.fetchall()
if rows:
    for r in rows:
        print(f"  Cliente: {r[0]}  |  Título: {r[1]}  |  Saldo: {r[2]}  |  Tipo: {r[3]}  |  Emissão: {r[4]}")
else:
    print("  NOT FOUND")

# 4. Who owns NF 46254 without zero-pad?
print(f"\n--- 4. bdnFaturamento WHERE [Nota Fiscal Número] = '46254' ---")
cursor.execute("""
    SELECT [Cliente Código], [Data de Emissão], [Nota Fiscal Número]
    FROM bdnFaturamento
    WHERE LTRIM(RTRIM([Nota Fiscal Número])) = '46254'
""")
rows = cursor.fetchall()
if rows:
    for r in rows:
        print(f"  Cliente Código: {r[0]}  |  Data Emissão: {r[1]}  |  NF: {r[2]}")
else:
    print("  NOT FOUND")

# 5. All bdnContasReceber entries for title 000046254 regardless of client
print(f"\n--- 5. ALL bdnContasReceber WHERE [Título Número]='{DOC}' ---")
cursor.execute("""
    SELECT [Cliente Código], [Título Número], [Valor Saldo], [Tipo Título], [Data de Emissão]
    FROM bdnContasReceber
    WHERE LTRIM(RTRIM([Título Número])) = ?
""", (DOC,))
rows = cursor.fetchall()
if rows:
    for r in rows:
        print(f"  Cliente: {r[0]}  |  Título: {r[1]}  |  Saldo: {r[2]}  |  Tipo: {r[3]}  |  Emissão: {r[4]}")
else:
    print("  NOT FOUND")

conn.close()
print("\n" + "=" * 80)
print("DONE")
