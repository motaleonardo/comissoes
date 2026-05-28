# Email Gerentes Por Filial Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change manager email attachments from one sheet per CEN to one sheet per filial, while preserving individual CEN email behavior.

**Architecture:** Keep recipient planning in `commission_tool/core/report_email.py` and attachment rendering in `commission_tool/ui/app.py`. Extend manager jobs to carry grouped reports by filial, derived from the CEN analytic data and the `Filial` column inside each analytic DataFrame.

**Tech Stack:** Python, pandas, Streamlit, openpyxl workbook generation, Microsoft Graph email sender.

---

## Decision

Use filial-based sheets only for manager emails:

- CEN emails remain unchanged: one CEN receives one analytic workbook for that CEN.
- Manager emails still have one email per manager.
- The manager attachment changes from "one sheet per CEN" to "one sheet per filial".
- Each filial sheet contains all analytic rows for the manager's CENs that belong to that filial.
- If a manager has CENs in multiple filiais, the workbook has one sheet for each filial.
- If a manager's own CEN exists only for manager commission calculation, it stays manager-only and appears in the manager's filial sheet.

This is better than one sheet per CEN because managers naturally review by branch/store, reduces workbook tab noise, and still preserves CEN identity inside rows through the existing `CEN` column.

## Edge Rules

- Use the `Filial` column in each CEN analytic DataFrame as the grouping source.
- If `Filial` is blank, group under `Sem filial`.
- Sort sheets alphabetically by filial.
- Sort rows inside each filial by `CEN`, `Nro Documento`, `Nro Chassi` when those columns exist.
- Keep production/test recipient rules unchanged.
- Keep Suelen as production CC for every email.
- Keep the manager-own-CEN rule unchanged: no CEN email, manager receives it.

## Files

- Modify `commission_tool/core/report_email.py`
  - Add `filial_reports` or a helper to group `ReportEmailJob.reports` by filial.
  - Keep dataclasses backward-compatible if possible.
- Modify `commission_tool/ui/app.py`
  - In `build_report_email_attachments`, use filial-grouped sheets for `job.kind == "manager"`.
- Modify `tests/test_report_email.py`
  - Add test ensuring manager job still receives all CEN reports.
  - Add test for manager-own-CEN still manager-only.
- Add or modify tests for attachment sheet grouping
  - Prefer `tests/test_report_email.py` if grouping helper is in `core/report_email.py`.
  - If grouping stays in UI, add a small unit test for `build_report_email_attachments`.

## Tasks

### Task 1: Add Failing Test For Manager Filial Grouping

- [ ] Add a test with two CEN reports for the same manager:
  - CEN A has rows in `Filial = Loja A`.
  - CEN B has rows in `Filial = Loja A` and `Filial = Loja B`.
- [ ] Assert the manager grouping returns two sheet groups: `Loja A` and `Loja B`.
- [ ] Assert `Loja A` contains rows from both CENs.
- [ ] Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_report_email
```

Expected: failure because filial grouping does not exist yet.

### Task 2: Implement Filial Grouping Helper

- [ ] Add a helper in `commission_tool/core/report_email.py`:

```python
def group_manager_reports_by_filial(reports: list[CENEmailReport]) -> list[tuple[str, pd.DataFrame]]:
    ...
```

- [ ] Build the grouped DataFrame by concatenating rows from each report.
- [ ] Ensure each output row keeps the existing `CEN` column. If a DataFrame lacks `CEN`, insert the report's `cen_name`.
- [ ] Sort output sheets by filial.
- [ ] Run the test again.

Expected: pass.

### Task 3: Use Filial Groups In Manager Attachment

- [ ] Modify `build_report_email_attachments` in `commission_tool/ui/app.py`.
- [ ] For `job.kind == "manager"`, call `group_manager_reports_by_filial(job.reports)`.
- [ ] Pass the returned `(filial, dataframe)` list to `build_multi_sheet_workbook`.
- [ ] Keep CEN attachment behavior unchanged.

### Task 4: Verify Existing Email Rules

- [ ] Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_graph_email tests.test_report_email tests.test_excel_chassis_history tests.test_postgres_mapping tests.test_reports
```

Expected: all tests pass.

### Task 5: Manual QA In Streamlit

- [ ] Open Relatórios.
- [ ] Generate reports for a known competence.
- [ ] Confirm the preview still lists CEN and manager recipients.
- [ ] Use `Enviar TESTE`.
- [ ] Confirm manager workbook tabs are filial names, not CEN names.
- [ ] Confirm CEN workbook remains individual.

## Open Question

If the same manager has the same filial name written with small variations, for example `Loja A` and `LOJA A`, normalize them into one sheet using case-insensitive matching, but display the first non-empty spelling encountered.
