import unittest

import pandas as pd

from commission_tool.core.report_email import build_report_email_plan, group_manager_reports_by_filial


class ReportEmailPlanTests(unittest.TestCase):
    def test_production_uses_cen_and_manager_emails_with_required_cc(self):
        cen_reports = [
            ("Ana CEN", "A001", pd.DataFrame({"Valor": [10]})),
            ("Bruno CEN", "B002", pd.DataFrame({"Valor": [20]})),
        ]
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "Ana CEN",
                    "gerente": "Maria Gerente",
                    "email": "ana@example.com",
                    "email_gerente": "maria@example.com",
                },
                {
                    "cod_vendedor": "B002",
                    "vendedor": "Bruno CEN",
                    "gerente": "Maria Gerente",
                    "email": "bruno@example.com",
                    "email_gerente": "maria@example.com",
                },
            ]
        )

        plan = build_report_email_plan(
            cen_reports,
            manager_relations,
            "Abril/2026",
            mode="production",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
        )

        self.assertEqual(plan.issues, [])
        self.assertEqual(len(plan.jobs), 3)
        cen_jobs = [job for job in plan.jobs if job.kind == "cen"]
        manager_jobs = [job for job in plan.jobs if job.kind == "manager"]
        self.assertEqual([job.to for job in cen_jobs], ["ana@example.com", "bruno@example.com"])
        self.assertEqual([job.cc for job in cen_jobs], [["suelen@example.com"], ["suelen@example.com"]])
        self.assertEqual(manager_jobs[0].to, "maria@example.com")
        self.assertEqual(manager_jobs[0].cc, ["suelen@example.com"])
        self.assertEqual([report.cen_name for report in manager_jobs[0].reports], ["Ana CEN", "Bruno CEN"])

    def test_test_mode_redirects_all_jobs_to_test_recipient_without_production_cc(self):
        cen_reports = [("Ana CEN", "A001", pd.DataFrame({"Valor": [10]}))]
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "Ana CEN",
                    "gerente": "Maria Gerente",
                    "email": "ana@example.com",
                    "email_gerente": "maria@example.com",
                }
            ]
        )

        plan = build_report_email_plan(
            cen_reports,
            manager_relations,
            "Abril/2026",
            mode="test",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
        )

        self.assertEqual(plan.issues, [])
        self.assertEqual(len(plan.jobs), 2)
        self.assertTrue(all(job.to == "leonardo@example.com" for job in plan.jobs))
        self.assertTrue(all(job.cc == [] for job in plan.jobs))

    def test_manager_own_cen_is_sent_only_as_manager_report(self):
        cen_reports = [("Maria Gerente", "M001", pd.DataFrame({"Valor": [10]}))]
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "M001",
                    "vendedor": "Maria Gerente",
                    "gerente": "Maria Gerente",
                    "email": "",
                    "email_gerente": "maria@example.com",
                }
            ]
        )

        plan = build_report_email_plan(
            cen_reports,
            manager_relations,
            "Abril/2026",
            mode="production",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
        )

        self.assertEqual(plan.issues, [])
        self.assertEqual(len(plan.jobs), 1)
        self.assertEqual(plan.jobs[0].kind, "manager")
        self.assertEqual(plan.jobs[0].to, "maria@example.com")
        self.assertEqual(plan.jobs[0].reports[0].cen_name, "Maria Gerente")

    def test_groups_manager_reports_by_filial_instead_of_cen_tabs(self):
        manager_relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "A001",
                    "vendedor": "Ana CEN",
                    "gerente": "Maria Gerente",
                    "email": "ana@example.com",
                    "email_gerente": "maria@example.com",
                },
                {
                    "cod_vendedor": "B002",
                    "vendedor": "Bruno CEN",
                    "gerente": "Maria Gerente",
                    "email": "bruno@example.com",
                    "email_gerente": "maria@example.com",
                },
            ]
        )
        cen_reports = [
            (
                "Ana CEN",
                "A001",
                pd.DataFrame(
                    [
                        {"Filial": "Loja A", "CEN": "Ana CEN", "Nro Documento": "1"},
                    ]
                ),
            ),
            (
                "Bruno CEN",
                "B002",
                pd.DataFrame(
                    [
                        {"Filial": "Loja A", "CEN": "Bruno CEN", "Nro Documento": "2"},
                        {"Filial": "Loja B", "CEN": "Bruno CEN", "Nro Documento": "3"},
                    ]
                ),
            ),
        ]
        plan = build_report_email_plan(
            cen_reports,
            manager_relations,
            "Abril/2026",
            mode="production",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
        )
        manager_job = next(job for job in plan.jobs if job.kind == "manager")

        filial_reports = group_manager_reports_by_filial(manager_job.reports)

        self.assertEqual([filial for filial, _ in filial_reports], ["Loja A", "Loja B"])
        loja_a = filial_reports[0][1]
        self.assertEqual(loja_a["CEN"].tolist(), ["Ana CEN", "Bruno CEN"])
        self.assertEqual(loja_a["Nro Documento"].tolist(), ["1", "2"])

    def test_adds_coordinator_email_job_with_consolidated_and_analytic_reports(self):
        plan = build_report_email_plan(
            [],
            pd.DataFrame(),
            "Abril/2026",
            mode="production",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
            coordinator_report_df=pd.DataFrame({"Tipo": ["Implemento"]}),
            coordinator_analytic_df=pd.DataFrame({"Nro Documento": ["1"]}),
            coordinator_email="wagner@hohl.com.br",
            coordinator_name="Wagner Goncalves Garcia",
        )

        self.assertEqual(plan.issues, [])
        self.assertEqual(len(plan.jobs), 1)
        self.assertEqual(plan.jobs[0].kind, "coordinator")
        self.assertEqual(plan.jobs[0].to, "wagner@hohl.com.br")
        self.assertEqual(plan.jobs[0].display_name, "Wagner Goncalves Garcia")
        self.assertEqual(len(plan.jobs[0].attachments), 2)


if __name__ == "__main__":
    unittest.main()
