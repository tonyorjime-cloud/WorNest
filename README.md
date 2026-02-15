# WorkNest Mini v3.2.1

**What’s new**
- **Future-year planning** for leave: nearest-in-rank rule is **relaxed** when the leave start date is in a future year (e.g., planning 2026 while still in 2025).
- **Unknown/variant rank strings** no longer block reliever selection (rank aliases handled).
- All previous v3.2 features preserved (auto task days, chess points, CSV import, tests & reports, etc.).

**Run**
```
pip install -r requirements.txt
streamlit run app.py
```
Login with staff email or name. Default password: `fcda`. Admin seed: `admin` / `fcda`.


## Task Attachments & Reminders (v3.2.2)
- Tasks now support attachments (PDF/DWG/DOC/XLS/etc.) stored under data/uploads/task_<id>/attachments.
- In-app reminder dashboard shows due-soon and overdue assignments.
- Optional email reminders: set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD (and optionally SMTP_FROM, SMTP_TLS).
- You can run reminders from the UI (admin) or via `python reminder_worker.py`.
