"""Commission period helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


MONTH_NAMES_PT = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


@dataclass(frozen=True)
class CommissionPeriod:
    label: str
    base_year: int
    base_month: int
    start_date: date
    end_date: date


def build_commission_period(base_year: int, base_month: int) -> CommissionPeriod:
    """Build the 16th-to-15th commission window for a base month."""
    if base_month == 12:
        end_year = base_year + 1
        end_month = 1
    else:
        end_year = base_year
        end_month = base_month + 1

    return CommissionPeriod(
        label=f"{MONTH_NAMES_PT[base_month]}/{base_year}",
        base_year=base_year,
        base_month=base_month,
        start_date=date(base_year, base_month, 16),
        end_date=date(end_year, end_month, 15),
    )


def build_period_options(
    reference_date: date,
    years_back: int = 1,
    years_ahead: int = 1,
) -> list[CommissionPeriod]:
    """Return month options around the reference year."""
    periods = []
    for year in range(reference_date.year - years_back, reference_date.year + years_ahead + 1):
        for month in range(1, 13):
            periods.append(build_commission_period(year, month))
    return periods


def default_base_period(reference_date: date) -> CommissionPeriod:
    """Default to the month whose 16-15 window just closed."""
    if reference_date.month == 1:
        return build_commission_period(reference_date.year - 1, 12)
    return build_commission_period(reference_date.year, reference_date.month - 1)
