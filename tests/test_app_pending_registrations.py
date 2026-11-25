import unittest
from unittest.mock import patch

import pandas as pd

from commission_tool.ui import app


class FakeSessionState(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value


class PendingRegistrationAppTests(unittest.TestCase):
    def test_go_to_settings_switches_sidebar_view(self):
        state = FakeSessionState(nav_view="Apurações")

        with patch.object(app.st, "session_state", state):
            app.go_to_settings()

        self.assertEqual(state["nav_view"], "Configurações")

    def test_refresh_with_reload_rules_reads_rules_again_even_when_cached(self):
        cached = pd.DataFrame([{"modelo": "VELHO", "percentual": 1.0}])
        fresh_fat = pd.DataFrame([{"modelo": "6110J", "percentual": 0.5}])
        fresh_relations = pd.DataFrame([{"cod_vendedor": "001", "gerente": "G1"}])
        state = FakeSessionState(
            machine_raw_df=pd.DataFrame(
                [
                    {
                        "Classificação Venda": "Maquinas JD - Novos",
                        "Modelo": "6110J",
                        "Nro Chassi": "CH1",
                        "Nro Documento": "1",
                        "Cod Vendedor": "001",
                        "Receita Bruta": 1000.0,
                    }
                ]
            ),
            machine_rate_fat_df=cached,
            machine_rate_margin_df=cached,
            manager_relations_df=pd.DataFrame(),
            machine_pending_dialog_shown=True,
        )

        with patch.object(app.st, "session_state", state), patch.object(
            app, "read_model_fat_rates", return_value=fresh_fat
        ), patch.object(app, "read_model_margin_rates", return_value=pd.DataFrame()), patch.object(
            app, "read_manager_relations", return_value=fresh_relations
        ), patch.object(
            app, "read_paid_commission_chassis_summary", return_value=pd.DataFrame()
        ), patch.object(
            app, "read_paid_commission_document_summary", return_value=pd.DataFrame()
        ), patch.object(
            app, "read_excluded_commission_chassis_summary", return_value=pd.DataFrame()
        ), patch.object(
            app, "read_excluded_commission_document_summary", return_value=pd.DataFrame()
        ):
            app.refresh_machine_apuracao_state(reload_rules=True)

        self.assertIs(state["machine_rate_fat_df"], fresh_fat)
        self.assertIs(state["manager_relations_df"], fresh_relations)
        self.assertFalse(state["machine_pending_dialog_shown"])
        self.assertEqual(state["machine_df"].loc[0, "% Comissão Fat."], 0.5)
        self.assertTrue(state["machine_df"].loc[0, "Regra Gerente Encontrada"])


if __name__ == "__main__":
    unittest.main()
