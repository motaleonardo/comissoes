import unittest
from datetime import date

from commission_tool.core.periods import build_commission_period, build_period_options, default_base_period


class CommissionPeriodTests(unittest.TestCase):
    def test_builds_16_to_15_period_for_regular_month(self):
        period = build_commission_period(2026, 3)

        self.assertEqual(period.label, "Março/2026")
        self.assertEqual(period.start_date, date(2026, 3, 16))
        self.assertEqual(period.end_date, date(2026, 4, 15))

    def test_builds_16_to_15_period_for_december(self):
        period = build_commission_period(2026, 12)

        self.assertEqual(period.start_date, date(2026, 12, 16))
        self.assertEqual(period.end_date, date(2027, 1, 15))

    def test_builds_current_and_next_year_options(self):
        periods = build_period_options(date(2026, 4, 18), years_ahead=1)

        self.assertEqual(len(periods), 36)
        self.assertEqual(periods[0].label, "Janeiro/2025")
        self.assertEqual(periods[-1].label, "Dezembro/2027")

    def test_defaults_to_previous_month(self):
        period = default_base_period(date(2026, 4, 18))

        self.assertEqual(period.label, "Março/2026")
        self.assertEqual(period.start_date, date(2026, 3, 16))
        self.assertEqual(period.end_date, date(2026, 4, 15))


if __name__ == "__main__":
    unittest.main()
