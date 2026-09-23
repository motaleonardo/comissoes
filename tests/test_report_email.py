import unittest

import pandas as pd

from commission_tool.core.report_email import (
    CENEmailReport,
    ReportEmailJob,
    build_report_email_plan,
    group_manager_reports_by_filial,
)
from commission_tool.ui.app import build_report_email_attachments


class ReportEmailPlanTests(unittest.TestCase):
    def setUp(self):
        self.relations = pd.DataFrame(
            [
                {
                    "cod_vendedor": "AAETBU",
                    "vendedor": "ABNER SAMUEL FERREIRA COSTA",
                    "gerente": "GERENTE TESTE",
                    "email": "abner@example.com",
                    "email_gerente": "gerente@example.com",
                }
            ]
        )

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

    def test_cen_job_gets_consorcio_attachment(self):
        consorcio = {
            "AAETBU": pd.DataFrame([{"Cota": "0025", "Parcela": "001 / 003", "Valor": 1680.0}])
        }

        plan = build_report_email_plan(
            [("ABNER SAMUEL FERREIRA COSTA", "AAETBU", pd.DataFrame([{"Nro Documento": "1"}]))],
            self.relations,
            "Setembro/2026",
            mode="test",
            test_recipient="teste@hohl.com.br",
            production_cc="",
            consorcio_by_cod_vendedor=consorcio,
        )

        cen_job = next(job for job in plan.jobs if job.kind == "cen")
        self.assertIn("Consórcio", [name for name, _ in cen_job.attachments])

    def test_cen_with_only_consorcio_still_gets_an_email_job(self):
        """Regression for I1: build_cen_analytic_reports never lists a CEN whose
        machine commission is zero, so a CEN who only sold consórcio this period was
        silently missing from the plan and from `issues`."""
        consorcio = {
            "AAETBU": pd.DataFrame([{"CEN": "ABNER SAMUEL FERREIRA COSTA", "Cota": "0025", "Valor": 1680.0}])
        }

        plan = build_report_email_plan(
            [],
            self.relations,
            "Setembro/2026",
            mode="production",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
            consorcio_by_cod_vendedor=consorcio,
        )

        self.assertEqual(plan.issues, [])
        cen_jobs = [job for job in plan.jobs if job.kind == "cen"]
        self.assertEqual(len(cen_jobs), 1)
        self.assertEqual(cen_jobs[0].to, "abner@example.com")
        self.assertIn("Consórcio", [name for name, _ in cen_jobs[0].attachments])

    def test_cen_with_only_consorcio_and_no_relation_raises_issue_instead_of_vanishing(self):
        consorcio = {"UNKNOWNCOD": pd.DataFrame([{"CEN": "SEM RELACAO", "Cota": "0025", "Valor": 1680.0}])}

        plan = build_report_email_plan(
            [],
            self.relations,
            "Setembro/2026",
            mode="production",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
            consorcio_by_cod_vendedor=consorcio,
        )

        self.assertEqual(len(plan.jobs), 0)
        self.assertTrue(
            any("UNKNOWNCOD" in issue and "sem relação" in issue for issue in plan.issues),
            plan.issues,
        )

    def test_manager_gets_consorcio_attachment_for_cen_without_machine_report(self):
        """Regression for I2: the manager report's "Gestor Consórcio" total sums every
        consortium row for that manager, but the e-mail attachment used to be built
        only from CENs that also had a machine-commission report."""
        consorcio = {
            "AAETBU": pd.DataFrame([{"CEN": "ABNER SAMUEL FERREIRA COSTA", "Cota": "0025", "Valor": 1680.0}])
        }

        plan = build_report_email_plan(
            [],
            self.relations,
            "Setembro/2026",
            mode="production",
            test_recipient="leonardo@example.com",
            production_cc="suelen@example.com",
            consorcio_by_cod_vendedor=consorcio,
        )

        manager_job = next(job for job in plan.jobs if job.kind == "manager")
        self.assertEqual(manager_job.to, "gerente@example.com")
        consorcio_attachment = next(df for name, df in manager_job.attachments if name == "Consórcio")
        self.assertEqual(consorcio_attachment["CEN"].tolist(), ["ABNER SAMUEL FERREIRA COSTA"])

    def test_consorcio_coordinator_gets_its_own_job(self):
        plan = build_report_email_plan(
            [],
            self.relations,
            "Setembro/2026",
            mode="production",
            test_recipient="",
            production_cc="",
            consorcio_coordinator_df=pd.DataFrame([{"Nome": "DJALMA", "Valor Comissão": 310.49}]),
            consorcio_coordinator_email="djalma.junior@hohl.com.br",
            consorcio_coordinator_name="DJALMA RODRIGUES DE PAULA JUNIOR",
        )

        job = next(job for job in plan.jobs if job.kind == "consorcio_coordinator")
        self.assertEqual(job.to, "djalma.junior@hohl.com.br")
        self.assertIn("Consórcio", job.subject)


class ReportEmailAttachmentsTests(unittest.TestCase):
    def test_cen_job_with_consorcio_attachment_produces_two_distinct_xlsx_files(self):
        report = CENEmailReport(cen_name="Ana CEN", cod_vendedor="A001", df=pd.DataFrame({"Valor": [10]}))
        job = ReportEmailJob(
            kind="cen",
            display_name="Ana CEN",
            to="ana@example.com",
            cc=[],
            subject="assunto",
            body="corpo",
            reports=[report],
            attachments=[("Consórcio", pd.DataFrame({"Cota": ["0025"]}))],
        )

        files = build_report_email_attachments(job, "Setembro/2026")

        self.assertEqual(len(files), 2)
        names = [name for name, _ in files]
        self.assertEqual(len(set(names)), 2, "os dois arquivos devem ter nomes distintos")
        for name, content in files:
            self.assertTrue(name.endswith(".xlsx"))
            self.assertGreater(len(content), 0)

    def test_consorcio_coordinator_job_produces_its_attachment_without_manager_path(self):
        job = ReportEmailJob(
            kind="consorcio_coordinator",
            display_name="DJALMA RODRIGUES DE PAULA JUNIOR",
            to="djalma.junior@hohl.com.br",
            cc=[],
            subject="Premiação de Consórcio - Setembro/2026",
            body="corpo",
            reports=[],
            attachments=[("Premiação Consórcio", pd.DataFrame({"Nome": ["DJALMA"], "Valor Comissão": [310.49]}))],
        )

        files = build_report_email_attachments(job, "Setembro/2026")

        self.assertEqual(len(files), 1)
        name, content = files[0]
        self.assertTrue(name.endswith(".xlsx"))
        self.assertGreater(len(content), 0)
        self.assertFalse(name.startswith("analiticos_gerente"))


if __name__ == "__main__":
    unittest.main()
