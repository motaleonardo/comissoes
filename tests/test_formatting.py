import unittest

from commission_tool.core.formatting import (
    format_currency_br,
    format_percent_br,
    parse_br_number,
    parse_percent_points,
)


class BrazilianFormattingTests(unittest.TestCase):
    def test_formats_currency_with_brazilian_separators(self):
        self.assertEqual(format_currency_br(15000), "R$ 15.000,00")
        self.assertEqual(format_currency_br(1234.5), "R$ 1.234,50")

    def test_formats_percent_as_percentage_points(self):
        self.assertEqual(format_percent_br(5), "5,00%")
        self.assertEqual(format_percent_br(0.2), "0,20%")

    def test_parses_brazilian_currency_text(self):
        self.assertEqual(parse_br_number("R$ 15.000,00"), 15000.0)

    def test_parses_percent_inputs_as_percentage_points(self):
        self.assertEqual(parse_percent_points("5,00%"), 5.0)
        self.assertEqual(parse_percent_points("0,20%"), 0.2)
        self.assertEqual(parse_percent_points(0.05), 5.0)
        self.assertEqual(parse_percent_points(5), 5.0)


if __name__ == "__main__":
    unittest.main()
