"""Inspect SQL Server tables/columns relevant to commission extraction."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from commission_tool.data.sources.sqlserver import get_connection


TABLE_PATTERNS = [
    "bdn",
    "Faturamento",
    "Incentiv",
    "Contas",
    "Comiss",
    "Meta",
    "Nota",
    "Vendedor",
    "Venda",
]


def main() -> None:
    load_dotenv()
    conn = get_connection(
        os.getenv("DB_SERVER", ""),
        os.getenv("DB_NAME", ""),
        use_windows_auth=True,
    )
    cursor = conn.cursor()

    where = " OR ".join("TABLE_NAME LIKE ?" for _ in TABLE_PATTERNS)
    cursor.execute(
        f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE ({where})
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """,
        [f"%{pattern}%" for pattern in TABLE_PATTERNS],
    )

    tables = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
    print(f"Found {len(tables)} candidate tables/views")
    for schema, table, table_type in tables:
        print(f"\n[{schema}].[{table}] ({table_type})")
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            (schema, table),
        )
        for column, data_type in cursor.fetchall():
            print(f"  - {column} ({data_type})")

    conn.close()


if __name__ == "__main__":
    main()
