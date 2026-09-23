"""Build email delivery plans for commission reports."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd


EmailMode = Literal["test", "production"]


@dataclass(frozen=True)
class CENEmailReport:
    cen_name: str
    cod_vendedor: str
    df: pd.DataFrame


@dataclass(frozen=True)
class ReportEmailJob:
    kind: Literal["cen", "manager", "coordinator", "consorcio_coordinator"]
    display_name: str
    to: str
    cc: list[str]
    subject: str
    body: str
    reports: list[CENEmailReport]
    attachments: list[tuple[str, pd.DataFrame]] = field(default_factory=list)


@dataclass(frozen=True)
class ReportEmailPlan:
    jobs: list[ReportEmailJob]
    issues: list[str]


def _normalize_code(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).strip())


def _match_key(value: object) -> str:
    return _normalize_text(value).casefold()


def _clean_email(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _job_recipient(mode: EmailMode, actual_email: str, test_recipient: str) -> str:
    return _clean_email(test_recipient) if mode == "test" else _clean_email(actual_email)


def _job_cc(mode: EmailMode, production_cc: str) -> list[str]:
    cc = _clean_email(production_cc)
    return [cc] if mode == "production" and cc else []


def group_manager_reports_by_filial(reports: list[CENEmailReport]) -> list[tuple[str, pd.DataFrame]]:
    grouped: dict[str, tuple[str, list[pd.DataFrame]]] = {}
    for report in reports:
        frame = report.df.copy() if report.df is not None else pd.DataFrame()
        if frame.empty:
            continue
        if "CEN" not in frame.columns:
            frame.insert(0, "CEN", report.cen_name)
        if "Filial" not in frame.columns:
            frame.insert(0, "Filial", "Sem filial")

        filial_values = frame["Filial"].fillna("").astype(str).str.strip()
        frame["Filial"] = filial_values.mask(filial_values.eq(""), "Sem filial")
        for filial, filial_df in frame.groupby("Filial", dropna=False, sort=False):
            filial_display = _normalize_text(filial) or "Sem filial"
            filial_key = filial_display.casefold()
            if filial_key not in grouped:
                grouped[filial_key] = (filial_display, [])
            grouped[filial_key][1].append(filial_df.copy())

    filial_reports: list[tuple[str, pd.DataFrame]] = []
    for _, (filial_display, frames) in grouped.items():
        filial_df = pd.concat(frames, ignore_index=True)
        sort_columns = [column for column in ["CEN", "Nro Documento", "Nro Chassi"] if column in filial_df.columns]
        if sort_columns:
            filial_df = filial_df.sort_values(sort_columns, na_position="last").reset_index(drop=True)
        filial_reports.append((filial_display, filial_df))

    return sorted(filial_reports, key=lambda item: item[0].upper())


def build_report_email_plan(
    cen_analytic_reports: list[tuple[str, str, pd.DataFrame]],
    manager_relations: pd.DataFrame | None,
    period_label: str,
    *,
    mode: EmailMode,
    test_recipient: str,
    production_cc: str,
    coordinator_report_df: pd.DataFrame | None = None,
    coordinator_analytic_df: pd.DataFrame | None = None,
    coordinator_email: str = "wagner@hohl.com.br",
    coordinator_name: str = "Wagner Goncalves Garcia",
    consorcio_by_cod_vendedor: dict[str, pd.DataFrame] | None = None,
    consorcio_coordinator_df: pd.DataFrame | None = None,
    consorcio_coordinator_email: str = "",
    consorcio_coordinator_name: str = "",
) -> ReportEmailPlan:
    """Create one email job per CEN and one grouped job per manager."""
    if mode not in {"test", "production"}:
        raise ValueError("mode must be 'test' or 'production'")

    relations = manager_relations.copy() if manager_relations is not None else pd.DataFrame()
    has_cen_reports = bool(cen_analytic_reports)
    has_coordinator_reports = coordinator_report_df is not None or coordinator_analytic_df is not None

    relation_by_code = {}
    if not relations.empty:
        relations["__cod_vendedor"] = relations.get("cod_vendedor", pd.Series(dtype=object)).apply(_normalize_code)
        relations["__gerente"] = relations.get("gerente", pd.Series(dtype=object)).apply(_normalize_text)
        relations["__vendedor"] = relations.get("vendedor", pd.Series(dtype=object)).apply(_normalize_text)
        relations["__email_cen"] = relations.get("email", pd.Series(dtype=object)).apply(_clean_email)
        relations["__email_gerente"] = relations.get("email_gerente", pd.Series(dtype=object)).apply(_clean_email)
        relations["__is_manager_own_cen"] = (
            relations["__gerente"].apply(_match_key).ne("")
            & relations["__gerente"].apply(_match_key).eq(relations["__vendedor"].apply(_match_key))
        )
        relations = relations[relations["__cod_vendedor"].ne("")].drop_duplicates("__cod_vendedor", keep="first")
        relation_by_code = relations.set_index("__cod_vendedor").to_dict("index")

    cc = _job_cc(mode, production_cc)
    issues: list[str] = []
    jobs: list[ReportEmailJob] = []
    manager_reports: dict[tuple[str, str], list[CENEmailReport]] = {}
    manager_consorcio: dict[tuple[str, str], list[pd.DataFrame]] = {}
    if relations.empty and has_cen_reports:
        issues.append("Tabela comissao_gerente_vendedor vazia.")
        cen_analytic_reports = []

    if mode == "test" and not _clean_email(test_recipient):
        issues.append("Configure REPORT_EMAIL_TEST_RECIPIENT para usar o modo teste.")
        return ReportEmailPlan(jobs=[], issues=issues)

    for cen_name, cod_vendedor, df in cen_analytic_reports:
        report = CENEmailReport(
            cen_name=_normalize_text(cen_name) or "(sem nome)",
            cod_vendedor=_normalize_code(cod_vendedor),
            df=df,
        )
        relation = relation_by_code.get(report.cod_vendedor)
        if relation is None:
            issues.append(f"CEN {report.cen_name} ({report.cod_vendedor}) sem relação em comissao_gerente_vendedor.")
            continue

        actual_cen_email = relation.get("__email_cen", "")
        actual_manager_email = relation.get("__email_gerente", "")
        manager_name = relation.get("__gerente", "")
        is_manager_own_cen = bool(relation.get("__is_manager_own_cen", False))

        consorcio_df = (consorcio_by_cod_vendedor or {}).get(report.cod_vendedor)
        cen_attachments = (
            [("Consórcio", consorcio_df)] if consorcio_df is not None and not consorcio_df.empty else []
        )

        if is_manager_own_cen:
            pass
        elif mode == "production" and not actual_cen_email:
            issues.append(f"CEN {report.cen_name} ({report.cod_vendedor}) sem e-mail do CEN.")
        else:
            jobs.append(
                ReportEmailJob(
                    kind="cen",
                    display_name=report.cen_name,
                    to=_job_recipient(mode, actual_cen_email, test_recipient),
                    cc=cc,
                    subject=f"Relatório analítico de comissão - {report.cen_name} - {period_label}",
                    body=(
                        f"Olá,\n\nSegue em anexo o relatório analítico de comissão de "
                        f"{report.cen_name} referente à competência {period_label}.\n\n"
                        "Atenciosamente."
                    ),
                    reports=[report],
                    attachments=cen_attachments,
                )
            )

        if mode == "production" and not actual_manager_email:
            issues.append(f"Gerente {manager_name or '(sem nome)'} sem e-mail de gestor.")
            continue

        manager_key = (manager_name, _job_recipient(mode, actual_manager_email, test_recipient))
        manager_reports.setdefault(manager_key, []).append(report)
        if cen_attachments:
            manager_consorcio.setdefault(manager_key, []).append(consorcio_df)

    for (manager_name, recipient), reports in sorted(manager_reports.items(), key=lambda item: item[0][0].upper()):
        manager_key = (manager_name, recipient)
        manager_consorcio_frames = manager_consorcio.get(manager_key, [])
        manager_attachments = (
            [("Consórcio", pd.concat(manager_consorcio_frames, ignore_index=True))]
            if manager_consorcio_frames
            else []
        )
        jobs.append(
            ReportEmailJob(
                kind="manager",
                display_name=manager_name or "(sem gerente)",
                to=recipient,
                cc=cc,
                subject=f"Relatórios analíticos dos CENs - {manager_name} - {period_label}",
                body=(
                    f"Olá,\n\nSegue em anexo o relatório analítico dos CENs vinculados "
                    f"a {manager_name} referente à competência {period_label}.\n\n"
                    "Atenciosamente."
                ),
                reports=reports,
                attachments=manager_attachments,
            )
        )

    if has_coordinator_reports:
        actual_coordinator_email = _clean_email(coordinator_email)
        if mode == "production" and not actual_coordinator_email:
            issues.append(f"Coordenador {coordinator_name} sem e-mail.")
        else:
            jobs.append(
                ReportEmailJob(
                    kind="coordinator",
                    display_name=coordinator_name,
                    to=_job_recipient(mode, actual_coordinator_email, test_recipient),
                    cc=cc,
                    subject=f"Relatórios de Implementos e Seminovos - {period_label}",
                    body=(
                        f"Olá,\n\nSegue em anexo o relatório consolidado e o analítico "
                        f"de Implementos e Seminovos referente à competência {period_label}.\n\n"
                        "Atenciosamente."
                    ),
                    reports=[],
                    attachments=[
                        ("Consolidado Implementos e Usados", coordinator_report_df if coordinator_report_df is not None else pd.DataFrame()),
                        ("Analítico Implementos e Usados", coordinator_analytic_df if coordinator_analytic_df is not None else pd.DataFrame()),
                    ],
                )
            )

    if consorcio_coordinator_df is not None and not consorcio_coordinator_df.empty:
        actual_email = _clean_email(consorcio_coordinator_email)
        if mode == "production" and not actual_email:
            issues.append(f"Coordenador de consórcio {consorcio_coordinator_name} sem e-mail.")
        else:
            jobs.append(
                ReportEmailJob(
                    kind="consorcio_coordinator",
                    display_name=consorcio_coordinator_name or "(sem nome)",
                    to=_job_recipient(mode, actual_email, test_recipient),
                    cc=cc,
                    subject=f"Premiação de Consórcio - {period_label}",
                    body=(
                        f"Olá,\n\nSegue em anexo a premiação de consórcio referente à "
                        f"competência {period_label}.\n\nAtenciosamente."
                    ),
                    reports=[],
                    attachments=[("Premiação Consórcio", consorcio_coordinator_df)],
                )
            )

    return ReportEmailPlan(jobs=jobs, issues=issues)
