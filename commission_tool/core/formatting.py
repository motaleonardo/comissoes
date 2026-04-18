"""Formatting and parsing helpers for Brazilian currency/percentage values."""

from __future__ import annotations

from typing import Any

import pandas as pd


def format_currency_br(value: Any) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return ""
    formatted = f"{float(number):,.2f}"
    return f"R$ {formatted}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_percent_br(value: Any) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return ""
    formatted = f"{float(number):,.2f}"
    return f"{formatted}%".replace(",", "X").replace(".", ",").replace("X", ".")


def parse_br_number(value: Any) -> float | None:
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None
    text = text.replace("R$", "").replace("%", "").strip()

    if "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def parse_percent_points(value: Any) -> float:
    """Parse percentage values into percentage points.

    Examples:
    - "5,00%" -> 5.0
    - "0,20%" -> 0.2
    - Excel percentage cell 5% as 0.05 -> 5.0
    - Plain numeric 5 -> 5.0
    """
    if pd.isna(value):
        return 0.0

    is_string = isinstance(value, str)
    parsed = parse_br_number(value)
    if parsed is None:
        return 0.0

    if not is_string and abs(parsed) <= 1 and parsed != 0:
        return parsed * 100
    return parsed

