import unittest

import pandas as pd

from commission_tool.ui.app import (
    build_machine_pay_review_df,
    build_machine_selection_editor_df,
    get_confirmed_pay_rows,
    get_machine_selected_rows,
    merge_machine_selection_state,
)


class AppMachineSelectionTests(unittest.TestCase):
    def test_build_machine_selection_editor_df_applies_existing_state(self):
        df_machine = pd.DataFrame(
            [
                {"CEN": "VENDEDOR A", "Nro Chassi": "CH1"},
                {"CEN": "VENDEDOR B", "Nro Chassi": "CH2"},
            ]
        )
        selection_state = {
            0: {"pagar": True, "excluir": False},
            1: {"pagar": False, "excluir": True},
        }

        editor_df = build_machine_selection_editor_df(df_machine, selection_state)

        self.assertEqual(editor_df["Pagar"].tolist(), [True, False])
        self.assertEqual(editor_df["Excluir"].tolist(), [False, True])

    def test_merge_machine_selection_state_preserves_hidden_rows(self):
        current_state = {
            0: {"pagar": True, "excluir": False},
            1: {"pagar": False, "excluir": True},
        }
        visible_df = pd.DataFrame(
            [
                {"Pagar": False, "Excluir": False},
            ],
            index=[0],
        )

        merged_state = merge_machine_selection_state(current_state, visible_df)

        self.assertNotIn(0, merged_state)
        self.assertIn(1, merged_state)
        self.assertEqual(merged_state[1], {"pagar": False, "excluir": True})

    def test_get_machine_selected_rows_uses_full_dataset_not_only_visible_rows(self):
        df_machine = pd.DataFrame(
            [
                {"CEN": "VENDEDOR A", "Nro Chassi": "CH1"},
                {"CEN": "VENDEDOR B", "Nro Chassi": "CH2"},
                {"CEN": "VENDEDOR C", "Nro Chassi": "CH3"},
            ]
        )
        selection_state = {
            0: {"pagar": True, "excluir": False},
            2: {"pagar": False, "excluir": True},
        }

        selected_to_pay, selected_to_exclude, pay_index, exclude_index = get_machine_selected_rows(
            df_machine,
            selection_state,
        )

        self.assertEqual(pay_index.tolist(), [0])
        self.assertEqual(exclude_index.tolist(), [2])
        self.assertEqual(selected_to_pay["Nro Chassi"].tolist(), ["CH1"])
        self.assertEqual(selected_to_exclude["Nro Chassi"].tolist(), ["CH3"])

    def test_pay_review_only_persists_rows_selected_in_apuracao_and_confirmed_in_review(self):
        selected_to_pay = pd.DataFrame(
            [
                {"Nro Chassi": "CH1", "Nro Documento": "1"},
                {"Nro Chassi": "CH2", "Nro Documento": "2"},
            ]
        )

        pay_review_df = build_machine_pay_review_df(selected_to_pay)
        edited_pay_review = pay_review_df.copy()
        edited_pay_review.loc[edited_pay_review["Nro Chassi"] == "CH2", "Confirmar Pagamento"] = False

        confirmed = get_confirmed_pay_rows(pay_review_df, edited_pay_review)

        self.assertEqual(confirmed["Nro Chassi"].tolist(), ["CH1"])
        self.assertEqual(confirmed["Nro Documento"].tolist(), ["1"])
        self.assertNotIn("Confirmar Pagamento", confirmed.columns)
        self.assertNotIn("__pay_review_row_id", confirmed.columns)


if __name__ == "__main__":
    unittest.main()
