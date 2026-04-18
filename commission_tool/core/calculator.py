"""Commission calculation entry points.

The concrete rules will be implemented after `regras_calculo.md` is finalized.
"""

from __future__ import annotations

import pandas as pd


def calculate_commission(sales: pd.DataFrame, rules) -> pd.DataFrame:
    """Calculate commissions from normalized sales data.

    This placeholder defines the future module boundary. The implementation
    should stay free of SQL and Streamlit code.
    """
    raise NotImplementedError("Commission calculation rules are not defined yet.")

