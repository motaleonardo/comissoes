"""Extraction and normalization pipeline.

This module will orchestrate SQL Server extraction, schema normalization and
joins before commission calculation.
"""

from __future__ import annotations

import pandas as pd

from commission_tool.data.sources.sqlserver import SQLServerDataSource


def normalize_sales(raw_sales: pd.DataFrame) -> pd.DataFrame:
    """Normalize extracted sales into the canonical calculation schema."""
    return raw_sales.copy()


def extract_machine_commission_base(conn, start_date, end_date) -> pd.DataFrame:
    """Extract the unified machine commission table from SQL Server."""
    return SQLServerDataSource(conn).extract_machine_commission_base(start_date, end_date)


def extract_incentive_titles(conn) -> pd.DataFrame:
    """Extract incentive titles and receivable status from SQL Server."""
    return SQLServerDataSource(conn).extract_incentive_titles_by_chassi()


def extract_incentive_summary(conn) -> pd.DataFrame:
    """Extract one incentive status row per chassis from SQL Server."""
    return SQLServerDataSource(conn).extract_incentive_summary_by_chassi()


def extract_machine_source_audit(conn, start_date, end_date) -> pd.DataFrame:
    """Extract source row/revenue audit for machine billing and returns."""
    return SQLServerDataSource(conn).extract_machine_source_audit(start_date, end_date)


def extract_machine_incentive_audit(conn, start_date, end_date) -> pd.DataFrame:
    """Extract incentive audit for chassis in the machine source selection."""
    return SQLServerDataSource(conn).extract_machine_incentive_audit(start_date, end_date)
