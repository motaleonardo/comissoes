import unittest

from commission_tool.core.incentives import classify_incentive_status


class IncentiveStatusTests(unittest.TestCase):
    def test_zero_balance_is_apto(self):
        status, detail = classify_incentive_status(0)

        self.assertEqual(status, "APTO")
        self.assertIn("quitados", detail)

    def test_positive_balance_is_not_apto(self):
        status, detail = classify_incentive_status(10)

        self.assertEqual(status, "NÃO APTO")
        self.assertIn("saldo pendente", detail)

    def test_missing_title_requires_verification(self):
        status, detail = classify_incentive_status(0, missing_title=True)

        self.assertEqual(status, "VERIFICAR")
        self.assertIn("não encontrado", detail)


if __name__ == "__main__":
    unittest.main()

