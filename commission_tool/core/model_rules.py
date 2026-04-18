"""Model-level commission configuration.

These rules will later be loaded from the Excel file provided by the user.
For now every percentage is zero, which keeps the extraction deterministic.
"""

from __future__ import annotations

import pandas as pd


MODEL_RULE_COLUMNS = [
    "Modelo",
    "% Comissão Fat.",
    "Meta de Margem",
    "% Comissão Margem",
]


def empty_model_rules() -> pd.DataFrame:
    return pd.DataFrame(columns=MODEL_RULE_COLUMNS)

