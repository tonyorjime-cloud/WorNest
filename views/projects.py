import json
import os
from datetime import date, datetime

import pandas as pd
import streamlit as st

from core.auth import current_staff_id, is_admin
from core.dates import _today, safe_parse_date
from core.db import execute, fetch_df
from core.labels import _biweekly_timing_status, _editable_status, _timing_status_label
from core.permissions import can_manage_projects
from core.projects_service import (
    CORE_DOC_CATEGORIES,
    STAGES,
    TEST_TYPES_DISPLAY,
    _BIWEEKLY_MODULES,
    _BIWEEKLY_STATUS_OPTIONS,
    _admin_phones,
    _attachment_rows,
    _attachment_rows_bulk,
    _biweekly_check_extra_rule,
    _biweekly_default_module_state,
    _biweekly_form_refresh,
    _build_biweekly_pdf_file,
    _ensure_biweekly_pdf,
    _legacy_sections_from_structured,
    _normalize_biweekly_structured_payload,
    _notify_admins_biweekly_submission,
    _project_is_dormant,
    _project_missing_historical_cycles,
    _project_open_biweekly_cycle,
    _project_visible_biweekly_reports_df,
    _render_attachment_list,
    _render_biweekly_structured_details,
    _save_biweekly_attachments,
    _save_test_result_attachment,
    _send_sms_notice,
    _structured_payload_to_json,
    _sync_biweekly_points_for_report,
    approve_biweekly_report,
    can_upload_core_to_project,
    can_upload_project_outputs,
    file_download_button,
    render_pdf_preview_and_download,
    save_uploaded_file,
)
from core.utils import _PDF_ERROR_PREFIX

def page_projects():
    st.markdown("<div class='worknest-header'><h2>🏗️ Projects</h2></div>", unsafe_allow_html=True)
    # For Staff: show posted projects first, then the rest.
    if is_admin() or can_manage_projects():
        projects=fetch_df("""
            SELECT p.id, p.code, p.name, p.client, p.location, p.start_date, p.end_date, p.next_due_date, p.supervisor_staff_id,
                   COALESCE(p.status,'ACTIVE') AS status, p.dormant_since, p.dormant_reason,
                   (SELECT name FROM staff s WHERE s.id=p.supervisor_staff_id) supervisor
            FROM projects p
            ORDER BY CASE WHEN COALESCE(p.status,'ACTIVE')='ACTIVE' THEN 0 ELSE 1 END, p.code
        """)
    else:
        sid = current_staff_id()
        projects=fetch_df("""
            SELECT p.id, p.code, p.name, p.client, p.location, p.start_date, p.end_date, p.next_due_date, p.supervisor_staff_id,
                   COALESCE(p.status,'ACTIVE') AS status, p.dormant_since, p.dormant_reason,
                   (SELECT name FROM staff s WHERE s.id=p.supervisor_staff_id) supervisor,
                   CASE WHEN EXISTS (
                        SELECT 1 FROM project_staff ps
                        WHERE ps.project_id = p.id AND ps.staff_id = ?
                   ) THEN 0 ELSE 1 END AS _posted_sort
            FROM projects p
            ORDER BY _posted_sort, p.code
        """, (sid,))

    # Filter large project lists
    proj_filter = st.radio("Project view", ["Active", "Dormant", "All"], horizontal=True, key="proj_filter")
    if not projects.empty:
        ps = projects["status"].astype(str).str.upper()
        if proj_filter == "Active":
            projects = projects[ps == "ACTIVE"]
        elif proj_filter == "Dormant":
            projects = projects[ps == "DORMANT"]

    # Admin control: global reset of next bi-weekly due dates
    if is_admin():
        with st.expander("🛠️ Admin: Reset live bi-weekly cycle (global)", expanded=False):
            st.caption("This realigns every project to the same live reporting baseline. It updates each project's next due date and also resets the global cycle anchor used for report numbering and compliance.")
            reset_date = st.date_input("Set live cycle start / next due date for all projects", value=date(2026,4,14), key="global_due_reset")
            if st.button("Apply global reset", key="btn_global_due_reset"):
                execute("UPDATE projects SET next_due_date=?", (str(reset_date),))
                execute("""INSERT INTO app_settings(key,value) VALUES(?,?)
                           ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""", ("GLOBAL_BIWEEKLY_NEXT_DUE", str(reset_date)))
                execute("""INSERT INTO app_settings(key,value) VALUES(?,?)
                           ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""", ("BIWEEKLY_CYCLE_START_DATE", str(reset_date)))
                st.success(f"Updated all projects: next due date and cycle anchor set to {reset_date}.")
                st.rerun()

    left,right=st.columns([1,2])
    with left:
        st.subheader("Project List")
        if projects.empty:
            st.info("No projects yet. Use the form on the right to add one.")
            selected=None
        else:
            labels=[f"{r['code']}  {r['name']}" for _,r in projects.iterrows()]
            pre_idx = 0
            try:
                target_pid = st.session_state.get("selected_project_id")
                if target_pid is not None:
                    match_idx = next((i for i, (_, rr) in enumerate(projects.iterrows()) if int(rr["id"]) == int(target_pid)), None)
                    if match_idx is not None:
                        pre_idx = match_idx
            except Exception:
                pre_idx = 0
            selected_label=st.selectbox("Select a project", labels, index=pre_idx if labels else 0, key="proj_select")
            selected=projects.iloc[labels.index(selected_label)] if labels else None
    with right:
        st.subheader("Create / Update Project")
        if not can_manage_projects():
            st.info("Only Admin can create/update/delete projects.")

        # Streamlit widgets keep values by key; so ensure keys vary by selected project.
        # This makes the right-side panel instantly reflect the selected project.
        suffix = f"_{int(selected['id'])}" if selected is not None else "_new"

        staff=fetch_df("SELECT id,name,section FROM staff ORDER BY name")
        sup_names=[""]+[s for s in staff["name"].tolist()] if not staff.empty else [""]
        code=st.text_input("Code", value=(selected["code"] if selected is not None else ""), key=f"proj_code{suffix}")
        name=st.text_input("Name", value=(selected["name"] if selected is not None else ""), key=f"proj_name{suffix}")
        client=st.text_input("Client", value=(selected["client"] if selected is not None and pd.notna(selected["client"]) else ""), key=f"proj_client{suffix}")
        location=st.text_input("Location", value=(selected["location"] if selected is not None and pd.notna(selected["location"]) else ""), key=f"proj_loc{suffix}")
        start=st.date_input("Start Date", value=date.today(), key="lv_start")
        end=st.date_input("End Date", value=(safe_parse_date(selected["end_date"], date.today()) if selected is not None else date.today()), key=f"proj_end{suffix}")
        sup_default = selected["supervisor"] if (selected is not None and pd.notna(selected["supervisor"])) else ""
        sup_name=st.selectbox("Supervisor", sup_names, index=sup_names.index(sup_default) if sup_default in sup_names else 0, key=f"proj_sup{suffix}")
        status_opts = ["ACTIVE", "DORMANT"]
        p_status = st.selectbox("Project Status", status_opts, index=(status_opts.index(str(selected.get('status') or 'ACTIVE').upper()) if selected is not None and str(selected.get('status') or 'ACTIVE').upper() in status_opts else 0), key=f"proj_status{suffix}")
        dormant_since = st.text_input("Dormant Since (YYYY-MM-DD)", value=(selected.get('dormant_since') if selected is not None and pd.notna(selected.get('dormant_since')) else ""), key=f"proj_dormant_since{suffix}") if p_status == "DORMANT" else ""
        dormant_reason = st.text_input("Dormant Reason", value=(selected.get('dormant_reason') if selected is not None and pd.notna(selected.get('dormant_reason')) else ""), key=f"proj_dormant_reason{suffix}") if p_status == "DORMANT" else ""
        colA,colB=st.columns(2)
        with colA:
            if can_manage_projects() and st.button("💾 Save / Update", key=f"proj_save{suffix}"):
                if selected is None:
                    sup_id=None
                    if sup_name!="": sup_id=int(staff[staff["name"]==sup_name]["id"].iloc[0])
                    execute("""INSERT INTO projects (code,name,client,location,start_date,end_date,supervisor_staff_id,status,dormant_since,dormant_reason)
                               VALUES (?,?,?,?,?,?,?,?,?,?)""", (code,name,client or None,location or None,str(start),str(end),sup_id,p_status,dormant_since or None,dormant_reason or None))
                    st.success("Project created.")
                else:
                    sup_id=None
                    if sup_name!="": sup_id=int(staff[staff["name"]==sup_name]["id"].iloc[0])
                    execute("""UPDATE projects SET code=?,name=?,client=?,location=?,start_date=?,end_date=?,supervisor_staff_id=?,status=?,dormant_since=?,dormant_reason=? WHERE id=?""",
                            (code,name,client or None,location or None,str(start),str(end),sup_id,p_status,(dormant_since or None) if p_status=='DORMANT' else None,(dormant_reason or None) if p_status=='DORMANT' else None,int(selected["id"])))
                    st.success("Project updated.")
                st.rerun()
        with colB:
            if (selected is not None) and st.button("🗑️ Delete", key=f"proj_del{suffix}"):
                execute("DELETE FROM projects WHERE id=?", (int(selected["id"]),))
                st.success("Project deleted."); st.rerun()

    if selected is not None:
        pid=int(selected["id"])
        st.markdown("### Posted Staff")
        df=fetch_df("""
            SELECT s.name, s.rank, ps.role
            FROM project_staff ps JOIN staff s ON s.id=ps.staff_id
            WHERE ps.project_id=? ORDER BY s.rank, s.name
        """,(pid,))
        st.dataframe(df if not df.empty else pd.DataFrame(columns=["name","rank","role"]), width='stretch')

        st.markdown("---")
        tabs = st.tabs([" Buildings"," Core Docs"," Tests"," Biweekly Reports"])

        # Buildings
        with tabs[0]:
            bdf=fetch_df("SELECT id,name,floors FROM buildings WHERE project_id=? ORDER BY name",(pid,))
            st.subheader("Buildings")
            st.dataframe(bdf if not bdf.empty else pd.DataFrame(columns=["id","name","floors"]), width='stretch')
            st.markdown("**Add / Edit Building**")
            names = [" New "] + (bdf["name"].tolist() if not bdf.empty else [])
            pick = st.selectbox("Choose building", names, key="b_pick")
            if pick==" New ":
                b_name = st.text_input("Building name", key="b_name_new")
                floors = st.number_input("Floors", 0, 200, 0, key="b_f_new")
                if st.button("? Add Building", key="b_add"):
                    execute("INSERT INTO buildings (project_id,name,floors) VALUES (?,?,?)",(pid,b_name,int(floors)))
                    st.success("Building added."); st.rerun()
            else:
                brow = bdf[bdf["name"]==pick].iloc[0]
                b_name = st.text_input("Building name", value=brow["name"], key="b_name_edit")
                floors = st.number_input("Floors", 0, 200, int(brow["floors"]), key="b_f_edit")
                colx,coly=st.columns(2)
                with colx:
                    if st.button("💾 Save Building", key="b_save"):
                        execute("UPDATE buildings SET name=?, floors=? WHERE id=?", (b_name,int(floors),int(brow["id"])))
                        st.success("Building updated."); st.rerun()
                with coly:
                    if st.button("🗑️ Delete Building", key="b_del"):
                        execute("DELETE FROM buildings WHERE id=?", (int(brow["id"]),))
                        st.success("Building deleted."); st.rerun()

        # Core Documents upload/list
        with tabs[1]:
            st.subheader("Core Documents")
            c1,c2=st.columns(2)
            with c1:
                st.caption("Required categories: " + ", ".join(CORE_DOC_CATEGORIES))
            with c2:
                allowed = can_upload_core_to_project(pid)
                st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)
            if allowed:
                cat = st.selectbox("Category", CORE_DOC_CATEGORIES, key="doc_cat")
                up = st.file_uploader("Upload file (PDF/Image)", type=["pdf","png","jpg","jpeg"], key="doc_file")
                if st.button("⬆️ Upload Document", key="doc_up"):
                    path=save_uploaded_file(up, f"project_{pid}/docs")
                    if path:
                        execute("""INSERT INTO documents (project_id, building_id, category, file_path, uploaded_at, uploader_staff_id)
                                   VALUES (?,?,?,?,?,?)""",(pid, None, cat, path, datetime.now().isoformat(timespec="seconds"), current_staff_id()))
                        st.success("Document uploaded.")
                    else:
                        st.error("Select a file first.")
            ddf=fetch_df("SELECT id,category,file_path,uploaded_at FROM documents WHERE project_id=? ORDER BY uploaded_at DESC",(pid,))
            if ddf.empty:
                st.info("No documents yet.")
            else:
                for _,r in ddf.iterrows():
                    colA,colB=st.columns([3,1])
                    with colA: st.write(f"**{r['category']}**  {os.path.basename(r['file_path'])}  \n*{r['uploaded_at']}*")
                    with colB: file_download_button("⬇️ Download", r["file_path"], key=f"docdl{r['id']}")

        # Tests upload/list
        with tabs[2]:
            st.subheader("Test Results (per building & stage)")
            allowed = can_upload_project_outputs(pid)
            st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)

            edit_test_id = st.session_state.get(f"test_edit_{pid}")
            edit_test = None
            if edit_test_id:
                tedit = fetch_df("SELECT * FROM test_results WHERE id=? AND project_id=?", (int(edit_test_id), pid))
                if not tedit.empty:
                    edit_test = tedit.iloc[0]

            bdf=fetch_df("SELECT id,name FROM buildings WHERE project_id=? ORDER BY name",(pid,))
            b_opts = [" (no specific building) "] + (bdf["name"].tolist() if not bdf.empty else [])
            selected_b = " (no specific building) "
            if edit_test is not None and pd.notna(edit_test.get('building_id')) and not bdf.empty:
                mt = bdf[bdf['id']==int(edit_test['building_id'])]
                if not mt.empty:
                    selected_b = str(mt['name'].iloc[0])
            with st.form(f"test_form_{pid}"):
                b_pick = st.selectbox("Building", b_opts, index=(b_opts.index(selected_b) if selected_b in b_opts else 0), key=f"t_building_{pid}")
                bid = None
                if b_pick!=" (no specific building) " and (not bdf.empty):
                    bid = int(bdf[bdf["name"]==b_pick]["id"].iloc[0])
                stage_default = str(edit_test.get('stage') or STAGES[0]) if edit_test is not None else STAGES[0]
                stage = st.selectbox("Stage", STAGES, index=(STAGES.index(stage_default) if stage_default in STAGES else 0), key=f"t_stage_{pid}")
                labels = [x[1] for x in TEST_TYPES_DISPLAY]
                reverse = {v:k for k,v in TEST_TYPES_DISPLAY}
                default_label = next((v for k,v in TEST_TYPES_DISPLAY if edit_test is not None and k == str(edit_test.get('test_type') or '')), labels[0])
                ttype_label = st.selectbox("Test Type", labels, index=(labels.index(default_label) if default_label in labels else 0), key=f"t_type_{pid}")
                ttype = reverse[ttype_label]
                batch_needed = (ttype in ["steel","reinforcement"])
                batch_id = st.text_input("Batch ID (required for batch tests)", value=(str(edit_test.get('batch_id') or '') if edit_test is not None else ''), key=f"t_batch_{pid}") if batch_needed else None
                test_date_val = safe_parse_date(edit_test.get('test_date'), date.today()) if edit_test is not None else date.today()
                test_date = st.date_input("Test Date", value=test_date_val, key=f"t_date_{pid}")
                result_summary = st.text_area("Result Summary / Notes", value=(str(edit_test.get('result_summary') or edit_test.get('notes') or '') if edit_test is not None else ''), key=f"t_summary_{pid}")
                attachment_caption = st.text_input("Attachment caption", value='', key=f"t_caption_{pid}")
                up = st.file_uploader("Upload test result file (PDF/Image)", type=["pdf","png","jpg","jpeg"], key=f"t_file_{pid}")
                c1, c2 = st.columns(2)
                submit_label = "💾 Update Test Result" if edit_test is not None else "⬆️ Save Test Result"
                save_test = c1.form_submit_button(submit_label)
                cancel_test = c2.form_submit_button("Cancel Edit") if edit_test is not None else False
            if cancel_test:
                st.session_state.pop(f"test_edit_{pid}", None)
                st.rerun()
            if save_test:
                if not allowed:
                    st.error("You don't have permission to upload to this project.")
                elif batch_needed and (not batch_id or not str(batch_id).strip()):
                    st.error("Batch ID is required for steel/reinforcement tests.")
                else:
                    now_iso = datetime.now().isoformat(timespec='seconds')
                    if edit_test is not None and _editable_status(edit_test.get('status')) and int(edit_test.get('uploader_staff_id') or -1) == int(current_staff_id() or -999) or is_admin():
                        path = str(edit_test.get('file_path') or '')
                        if up is not None:
                            path = save_uploaded_file(up, f"project_{pid}/tests") or path
                        execute("UPDATE test_results SET building_id=?, stage=?, test_type=?, batch_id=?, file_path=?, test_date=?, result_summary=?, notes=?, updated_at=?, status=? WHERE id=?",
                                (bid, stage, ttype, batch_id, path, str(test_date), result_summary, result_summary, now_iso, 'PENDING', int(edit_test['id'])))
                        rid = int(edit_test['id'])
                        if up is not None:
                            _save_test_result_attachment(rid, up, attachment_caption, pid=pid)
                        admin_numbers = _admin_phones()
                        if admin_numbers:
                            _send_sms_notice(
                                admin_numbers,
                                f"WorkNest: Test result resubmitted for {selected['code']} ({str(test_date)}). Please review."
                            )
                        st.success("Test result updated and resubmitted for admin review.")
                        st.session_state.pop(f"test_edit_{pid}", None)
                        st.rerun()
                    else:
                        path = save_uploaded_file(up, f"project_{pid}/tests") if up is not None else ''
                        rid = execute("""INSERT INTO test_results (project_id,building_id,stage,test_type,batch_id,file_path,uploaded_at,uploader_staff_id,test_date,result_summary,notes,status,updated_at)
                                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                      (pid, bid, stage, ttype, batch_id, path or '', now_iso, current_staff_id(), str(test_date), result_summary, result_summary, 'PENDING', now_iso))
                        if up is not None:
                            _save_test_result_attachment(rid, up, attachment_caption, pid=pid)
                        admin_numbers = _admin_phones()
                        if admin_numbers:
                            _send_sms_notice(
                                admin_numbers,
                                f"WorkNest: New test result submitted for {selected['code']} ({str(test_date)}). Please review."
                            )
                        st.success("Test result saved  pending admin approval.")
                        st.rerun()

            tdf=fetch_df("""
                SELECT tr.id, b.name AS building, tr.building_id, tr.stage, tr.test_type, tr.batch_id, tr.file_path, tr.uploaded_at, tr.test_date,
                       COALESCE(tr.result_summary,tr.notes,'') AS result_summary, COALESCE(tr.status,'APPROVED') AS status, tr.uploader_staff_id
                FROM test_results tr
                LEFT JOIN buildings b ON b.id=tr.building_id
                WHERE tr.project_id=?
                  AND (COALESCE(tr.status,'APPROVED')='APPROVED' OR tr.uploader_staff_id=? OR ?=1)
                ORDER BY COALESCE(tr.updated_at,tr.uploaded_at) DESC, tr.id DESC
            """,(pid, current_staff_id() or -1, 1 if is_admin() else 0))
            if tdf.empty:
                st.info("No tests uploaded yet.")
            else:
                for _,r in tdf.iterrows():
                    ctop, cstat = st.columns([4,1])
                    bname = r["building"] if pd.notna(r["building"]) else ""
                    lab = str(r["test_type"]).capitalize()
                    if r["test_type"] in ["steel","reinforcement"] and pd.notna(r["batch_id"]):
                        lab += f" (Batch: {r['batch_id']})"
                    with ctop:
                        st.markdown(f"**{lab}** — Building: {bname} — Stage: {r['stage']}  \n**Test Date:** {r.get('test_date') or '—'}  \n{r.get('result_summary') or ''}")
                    with cstat:
                        st.markdown(f"<span class='pill'>{r['status']}</span>", unsafe_allow_html=True)
                    cols = st.columns([1,1,1,1])
                    if str(r.get('file_path') or '').strip():
                        with cols[0]:
                            file_download_button("⬇️ Main File", r["file_path"], key=f"tdl{r['id']}")
                    editable = _editable_status(r.get('status')) and (is_admin() or int(r.get('uploader_staff_id') or -1) == int(current_staff_id() or -999))
                    if editable:
                        with cols[1]:
                            if st.button("✏️ Edit", key=f"tedit_{r['id']}"):
                                st.session_state[f"test_edit_{pid}"] = int(r['id'])
                                refreshed = fetch_df("SELECT * FROM biweekly_reports WHERE id=?", (int(rid),))
                                if not refreshed.empty:
                                    pdf_path = _ensure_biweekly_pdf(refreshed.iloc[0].to_dict(), project_name=f"{selected['code']} — {selected['name']}", attachments_df=_attachment_rows('biweekly', int(rid)))
                                    if pdf_path.startswith(_PDF_ERROR_PREFIX):
                                        st.warning("Historical report saved, but PDF generation failed. Open the report later to retry after the server issue is fixed.")
                                _notify_admins_biweekly_submission(selected['code'], chosen['cycle_no'], "submitted")
                                st.rerun()
                    if is_admin():
                        with cols[2]:
                            if r['status']!='APPROVED' and st.button("✅ Approve", key=f"tapp{r['id']}"):
                                execute("UPDATE test_results SET status='APPROVED', reviewed_by_staff_id=?, reviewed_at=?, approved_at=?, approved_by_staff_id=? WHERE id=?",
                                        (current_staff_id(), datetime.now().isoformat(timespec='seconds'), datetime.now().isoformat(timespec='seconds'), current_staff_id(), int(r['id'])))
                                st.rerun()
                        with cols[3]:
                            if r['status']!='REJECTED' and st.button("⛔ Reject", key=f"trej{r['id']}"):
                                execute("UPDATE test_results SET status='REJECTED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                        (current_staff_id(), datetime.now().isoformat(timespec='seconds'), int(r['id'])))
                                st.rerun()
                    _render_attachment_list('test', int(r['id']), f"tatt_{r['id']}")
                    st.markdown('---')

        # Biweekly Reports
        with tabs[3]:
            st.subheader("Biweekly Reports")
            allowed = can_upload_project_outputs(pid)
            st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)
            if _project_is_dormant(pid):
                st.info("This project is dormant. Biweekly reporting is ignored while dormant.")
                current_cycle, cycle_reason = None, "Dormant project"
            else:
                current_cycle, cycle_reason = _project_open_biweekly_cycle(pid)
                if current_cycle is not None:
                    st.markdown(
                        f"""
                        <div style="background:#f8f9fa;border:1px solid #d9d9d9;border-radius:10px;padding:14px 16px;margin:8px 0 12px 0;color:#111;line-height:1.65;">
                          <div style="font-size:1.05rem;font-weight:700;margin-bottom:8px;">Current Reporting Window</div>
                          <div><strong>Report No:</strong> {current_cycle['cycle_no']}</div>
                          <div><strong>Reporting Period:</strong> {current_cycle['window_start'].isoformat()}  {current_cycle['window_end'].isoformat()}</div>
                          <div><strong>Submission Deadline:</strong> Tue {current_cycle['due_date'].isoformat()}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    latest_cycle_df = fetch_df(
                        """SELECT COALESCE(status,'PENDING') AS status, rejected_reason, review_note
                           FROM biweekly_reports
                           WHERE project_id=? AND cycle_no=?
                           ORDER BY COALESCE(updated_at, reviewed_at, submitted_on, uploaded_at, report_date) DESC, id DESC
                           LIMIT 1""",
                        (pid, int(current_cycle["cycle_no"])),
                    )
                    if not latest_cycle_df.empty and str(latest_cycle_df.iloc[0].get("status") or "").upper() == "REJECTED":
                        st.warning("Previous report was rejected. Please submit a new report.")
                        reject_reason = str(latest_cycle_df.iloc[0].get("rejected_reason") or latest_cycle_df.iloc[0].get("review_note") or "").strip()
                        if reject_reason:
                            st.caption(f"Rejection reason: {reject_reason}")
                else:
                    st.info(cycle_reason or "No report window is currently available for upload.")

            hist = _project_missing_historical_cycles(pid)
            if hist:
                with st.expander("Historical Backfill (old report cycles)", expanded=False):
                    options = {f"Report {c['cycle_no']}  {c['window_start'].isoformat()}  {c['window_end'].isoformat()}  Due {c['due_date'].isoformat()}": c for c in hist}
                    pick = st.selectbox("Historical cycle", list(options.keys()), key=f"hist_pick_{pid}")
                    chosen = options[pick]
                    manual_sub = st.date_input("Actual submission date", value=chosen['due_date'], key=f"hist_sub_{pid}")
                    up_hist = st.file_uploader("Upload historical biweekly report (PDF/Image)", type=["pdf","png","jpg","jpeg"], key=f"bw_hist_file_{pid}")
                    if st.button("⬆️ Upload Historical Report", key=f"bw_hist_up_{pid}"):
                        if not allowed:
                            st.error("You don't have permission to upload to this project.")
                        else:
                            path = save_uploaded_file(up_hist, f"project_{pid}/reports")
                            if path:
                                timing_status = _biweekly_timing_status(chosen['due_date'], manual_sub)
                                rid = execute(
                                    "INSERT INTO biweekly_reports (project_id,report_date,file_path,uploaded_at,submitted_on,uploader_staff_id,status,cycle_no,window_start,window_end,due_date,timing_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (pid, str(chosen['due_date']), path, datetime.now().isoformat(timespec='seconds'), str(manual_sub), current_staff_id(), 'PENDING', int(chosen['cycle_no']), str(chosen['window_start']), str(chosen['window_end']), str(chosen['due_date']), timing_status)
                                )
                                st.success(f"Historical report uploaded. Status: {_timing_status_label(timing_status)}  pending admin approval.")
                                st.rerun()
                            else:
                                st.error("Select a file first.")

            edit_report_id = st.session_state.get(f"bw_edit_{pid}")
            edit_report = None
            if edit_report_id:
                edf = fetch_df("SELECT * FROM biweekly_reports WHERE id=? AND project_id=?", (int(edit_report_id), pid))
                if not edf.empty:
                    edit_report = edf.iloc[0]
            elif current_cycle is not None and current_staff_id() is not None:
                draft_df = fetch_df("""SELECT * FROM biweekly_reports
                                       WHERE project_id=? AND uploader_staff_id=? AND cycle_no=? AND COALESCE(status,'PENDING') IN ('SUBMITTED','NEEDS_REVISION','PENDING')
                                       ORDER BY id DESC LIMIT 1""",
                                    (pid, current_staff_id(), int(current_cycle['cycle_no'])))
                if not draft_df.empty:
                    edit_report = draft_df.iloc[0]

            if current_cycle is not None or edit_report is not None:
                default_cycle = int(edit_report['cycle_no']) if edit_report is not None and pd.notna(edit_report.get('cycle_no')) else (int(current_cycle['cycle_no']) if current_cycle is not None else None)
                default_start = safe_parse_date(edit_report.get('window_start'), current_cycle['window_start'] if current_cycle is not None else date.today()) if edit_report is not None else (current_cycle['window_start'] if current_cycle is not None else date.today())
                default_end = safe_parse_date(edit_report.get('window_end'), current_cycle['window_end'] if current_cycle is not None else date.today()) if edit_report is not None else (current_cycle['window_end'] if current_cycle is not None else date.today())
                default_due = safe_parse_date(edit_report.get('due_date'), current_cycle['due_date'] if current_cycle is not None else date.today()) if edit_report is not None else (current_cycle['due_date'] if current_cycle is not None else date.today())
                st.markdown("#### Complete report in WorkNest")
                st.caption("This report can be edited until admin approval. Once approved, it becomes locked.")
                if edit_report is not None and str(edit_report.get("status") or "").upper() == "NEEDS_REVISION":
                    st.warning(f"Revision requested: {edit_report.get('review_note') or 'Please review and update this report before resubmitting.'}")
                cmeta1, cmeta2, cmeta3 = st.columns(3)
                cmeta1.text_input("Report No.", value=(str(default_cycle) if default_cycle is not None else ''), disabled=True, key=f"bw_cycle_{pid}")
                cmeta2.text_input("Reporting Window", value=f"{default_start}  {default_end}", disabled=True, key=f"bw_window_{pid}")
                cmeta3.text_input("Submission Deadline", value=str(default_due), disabled=True, key=f"bw_due_{pid}")
                report_date_val = safe_parse_date(edit_report.get('report_date'), default_due) if edit_report is not None else default_due
                structured_existing = _normalize_biweekly_structured_payload(edit_report.get('structured_report_json') if edit_report is not None else None)
                default_selected_modules = structured_existing.get("selected_modules") or []
                module_options = list(_BIWEEKLY_MODULES.keys())
                bw_form_scope = f"{pid}_{int(edit_report['id'])}" if edit_report is not None and pd.notna(edit_report.get('id')) else f"{pid}_new_{default_cycle or 'draft'}"
                selected_modules = st.multiselect(
                    "Observed structural activities",
                    options=module_options,
                    default=default_selected_modules,
                    format_func=lambda k: _BIWEEKLY_MODULES[k]["label"],
                    key=f"bw_modules_{bw_form_scope}",
                    help="Select only the activities actually observed on site for this reporting visit.",
                )
                if selected_modules:
                    st.caption("Complete the compliance checkpoints for each observed activity.")
                else:
                    st.info("Select at least one observed activity to open the structured inspection sections.")

                with st.container():
                    report_date = st.date_input("Report Date", value=report_date_val, key=f"bw_report_date_{bw_form_scope}")
                    structured_payload = {"selected_modules": selected_modules, "modules": {}}
                    non_compliance_errors = []
                    for module_key in selected_modules:
                        meta = _BIWEEKLY_MODULES[module_key]
                        module_state = structured_existing["modules"].get(module_key, _biweekly_default_module_state(module_key))
                        with st.expander(meta["label"], expanded=True):
                            checks_state = {}
                            check_details_state = {}
                            extras_state = {}
                            for idx, (field_key, label) in enumerate(meta.get("checks", [])):
                                check_key = f"bw_{module_key}_{field_key}_{bw_form_scope}"
                                check_val = st.selectbox(
                                    label,
                                    _BIWEEKLY_STATUS_OPTIONS,
                                    index=max(0, _BIWEEKLY_STATUS_OPTIONS.index(module_state["checks"].get(field_key, "Not checked")) if module_state["checks"].get(field_key, "Not checked") in _BIWEEKLY_STATUS_OPTIONS else 2),
                                    key=check_key,
                                    on_change=_biweekly_form_refresh,
                                )
                                current_val = st.session_state.get(check_key, check_val)
                                checks_state[field_key] = current_val
                                existing_detail = str((module_state.get("check_details") or {}).get(field_key) or "")
                                if current_val == "Non-compliant":
                                    detail_val = st.text_area(
                                        f"Reason/details for {label}",
                                        value=existing_detail,
                                        height=90,
                                        key=f"bw_{module_key}_{field_key}_detail_{bw_form_scope}",
                                        help="Required when this checkpoint is marked Non-compliant.",
                                    )
                                    check_details_state[field_key] = detail_val
                                    if not str(detail_val or "").strip():
                                        non_compliance_errors.append(f"{meta['label']}: {label} requires a reason/details entry.")
                                else:
                                    check_details_state[field_key] = existing_detail
                                rule = _biweekly_check_extra_rule(module_key, field_key)
                                if rule:
                                    extra_key = rule["extra_key"]
                                    extra_label = rule["label"]
                                    existing_extra = str(module_state["extras"].get(extra_key) or "")
                                    if current_val == rule["required_status"]:
                                        extra_val = st.text_input(
                                            extra_label,
                                            value=existing_extra,
                                            key=f"bw_{module_key}_{extra_key}_{bw_form_scope}",
                                        )
                                        extras_state[extra_key] = extra_val
                                        if not str(extra_val or "").strip():
                                            non_compliance_errors.append(f"{meta['label']}: {extra_label} is required when {label} is marked {rule['required_status']}.")
                                    else:
                                        extras_state[extra_key] = existing_extra
                            for field_key, label in meta.get("extras", []):
                                if field_key in extras_state:
                                    continue
                                if field_key in meta.get("selects", {}):
                                    opts = meta["selects"][field_key]
                                    current_val = module_state["extras"].get(field_key, opts[0])
                                    extras_state[field_key] = st.selectbox(
                                        label,
                                        opts,
                                        index=max(0, opts.index(current_val) if current_val in opts else 0),
                                        key=f"bw_{module_key}_{field_key}_{bw_form_scope}",
                                    )
                                else:
                                    extras_state[field_key] = st.text_input(
                                        label,
                                        value=str(module_state["extras"].get(field_key) or ""),
                                        key=f"bw_{module_key}_{field_key}_{bw_form_scope}",
                                    )
                            remarks_val = st.text_area(
                                f"{meta['label']} remarks",
                                value=str(module_state.get("remarks") or ""),
                                height=80,
                                key=f"bw_{module_key}_remarks_{bw_form_scope}",
                            )
                            structured_payload["modules"][module_key] = {
                                "checks": checks_state,
                                "check_details": check_details_state,
                                "extras": extras_state,
                                "remarks": remarks_val,
                            }
                    hse_observations = st.text_area("HSE Observations", value=(str(edit_report.get('hse_observations') or '') if edit_report is not None else ''), height=80, key=f"bw_hse_{bw_form_scope}")
                    rfi_notes = st.text_area("RFI / EI Notes", value=(str(edit_report.get('rfi_notes') or '') if edit_report is not None else ''), height=80, key=f"bw_rfi_{bw_form_scope}")
                    general_remarks = st.text_area("General Remarks", value=(str(edit_report.get('general_remarks') or '') if edit_report is not None else ''), height=100, key=f"bw_rem_{bw_form_scope}")
                    legacy_sections = _legacy_sections_from_structured(structured_payload, hse_observations, rfi_notes, general_remarks)
                    site_activities = legacy_sections["site_activities"]
                    reinforcement_observations = legacy_sections["reinforcement_observations"]
                    concrete_observations = legacy_sections["concrete_observations"]
                    structured_report_json = _structured_payload_to_json(structured_payload)
                    st.markdown("**Images only**")
                    st.caption("Maximum of 5 images. On phones, use the Upload button to choose from gallery or camera. On computers, upload from storage only.")
                    photo_files = st.file_uploader("Upload images (maximum 5)", type=["png","jpg","jpeg"], accept_multiple_files=True, key=f"bw_files_{bw_form_scope}")
                    if photo_files and len(photo_files) > 5:
                        st.error("Maximum of 5 images allowed. Only the first 5 will be used.")
                        photo_files = photo_files[:5]
                    if photo_files:
                        st.caption(f"{len(photo_files)}/5 images selected")
                    caption_register = st.text_area("Image caption register (one line per image)", value='', help="Write captions line by line in the same order as the uploaded images.", key=f"bw_caps_{bw_form_scope}")
                    f1, f2 = st.columns(2)
                    submit_text = "💾 Update Report" if edit_report is not None else "⬆️ Submit Report"
                    do_save = f1.button(submit_text, key=f"bw_save_{bw_form_scope}")
                    do_cancel = f2.button("Cancel Edit", key=f"bw_cancel_{bw_form_scope}") if edit_report is not None else False
                if do_cancel:
                    st.session_state.pop(f"bw_edit_{pid}", None)
                    st.rerun()
                if do_save:
                    if not allowed:
                        st.error("You don't have permission to upload to this project.")
                    elif non_compliance_errors:
                        for msg in non_compliance_errors:
                            st.error(msg)
                    elif not selected_modules:
                        st.error("Select at least one observed activity before submitting the report.")
                    elif current_cycle is None and edit_report is None:
                        st.error(cycle_reason or "No open reporting cycle is available.")
                    else:
                        now_iso = datetime.now().isoformat(timespec='seconds')
                        target_cycle = default_cycle
                        target_start = default_start
                        target_end = default_end
                        target_due = default_due
                        submitted_on = _today()
                        timing_status = _biweekly_timing_status(target_due, submitted_on)
                        if edit_report is not None and (_editable_status(edit_report.get('status')) and (is_admin() or int(edit_report.get('uploader_staff_id') or -1) == int(current_staff_id() or -999))):
                            base_path = str(edit_report.get('file_path') or '')
                            first_new = save_uploaded_file(photo_files[0], f"project_{pid}/reports") if photo_files else None
                            if first_new:
                                base_path = first_new
                            execute("""UPDATE biweekly_reports
                                       SET report_date=?, file_path=?, updated_at=?, submitted_on=?, status=?, cycle_no=?, window_start=?, window_end=?, due_date=?, timing_status=?,
                                           site_activities=?, reinforcement_observations=?, concrete_observations=?, hse_observations=?, rfi_notes=?, general_remarks=?, selected_modules=?, structured_report_json=?, review_note=?, reviewed_at=?, reviewed_by_staff_id=?
                                       WHERE id=?""",
                                    (str(report_date), base_path or '', now_iso, str(submitted_on), 'SUBMITTED', int(target_cycle) if target_cycle is not None else None, str(target_start), str(target_end), str(target_due), timing_status,
                                     site_activities, reinforcement_observations, concrete_observations, hse_observations, rfi_notes, general_remarks, json.dumps(selected_modules), structured_report_json, None, None, None, int(edit_report['id'])))
                            _save_biweekly_attachments(int(edit_report['id']), photo_files, None, caption_register, pid=pid)
                            refreshed = fetch_df("SELECT * FROM biweekly_reports WHERE id=?", (int(edit_report['id']),))
                            if not refreshed.empty:
                                atts = _attachment_rows('biweekly', int(edit_report['id']))
                                pdf_path = _build_biweekly_pdf_file(refreshed.iloc[0].to_dict(), project_name=f"{selected['code']}  {selected['name']}", attachments_df=atts)
                                execute("UPDATE biweekly_reports SET report_pdf_path=? WHERE id=?", (pdf_path, int(edit_report['id'])))
                                if str(pdf_path).startswith(_PDF_ERROR_PREFIX):
                                    st.warning("Report updated, but PDF generation failed. Open the report later to retry after the server issue is fixed.")
                            _notify_admins_biweekly_submission(selected['code'], target_cycle, "resubmitted")
                            st.success("Report updated and resubmitted for admin review.")
                            st.session_state.pop(f"bw_edit_{pid}", None)
                            st.rerun()
                        else:
                            base_path = save_uploaded_file(photo_files[0], f"project_{pid}/reports") if photo_files else None
                            rid = execute(
                                """INSERT INTO biweekly_reports
                                   (project_id,report_date,file_path,uploaded_at,submitted_on,uploader_staff_id,status,cycle_no,window_start,window_end,due_date,timing_status,site_activities,reinforcement_observations,concrete_observations,hse_observations,rfi_notes,general_remarks,updated_at,selected_modules,structured_report_json)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (pid, str(report_date), base_path or '', now_iso, str(submitted_on), current_staff_id(), 'SUBMITTED', int(target_cycle) if target_cycle is not None else None, str(target_start), str(target_end), str(target_due), timing_status, site_activities, reinforcement_observations, concrete_observations, hse_observations, rfi_notes, general_remarks, now_iso, json.dumps(selected_modules), structured_report_json)
                            )
                            _save_biweekly_attachments(int(rid), photo_files, None, caption_register, pid=pid)
                            refreshed = fetch_df("SELECT * FROM biweekly_reports WHERE id=?", (int(rid),))
                            if not refreshed.empty:
                                atts = _attachment_rows('biweekly', int(rid))
                                pdf_path = _build_biweekly_pdf_file(refreshed.iloc[0].to_dict(), project_name=f"{selected['code']}  {selected['name']}", attachments_df=atts)
                                execute("UPDATE biweekly_reports SET report_pdf_path=? WHERE id=?", (pdf_path, int(rid)))
                                if str(pdf_path).startswith(_PDF_ERROR_PREFIX):
                                    st.warning("Report saved, but PDF generation failed. Open the report later to retry after the server issue is fixed.")
                            _notify_admins_biweekly_submission(selected['code'], target_cycle, "submitted")
                            st.success(f"Report saved. Status: {_timing_status_label(timing_status)}  pending admin approval.")
                            st.rerun()

            rdf = _project_visible_biweekly_reports_df(
                int(pid),
                1 if is_admin() else 0,
                int(current_staff_id() or -1),
            )
            if rdf.empty:
                st.info("No reports yet.")
            else:
                report_ids = tuple(int(x) for x in rdf["id"].tolist())
                bulk_atts = _attachment_rows_bulk('biweekly', report_ids)
                atts_by_parent = {}
                if not bulk_atts.empty:
                    for parent_id, grp in bulk_atts.groupby("parent_id", sort=False):
                        atts_by_parent[int(parent_id)] = grp.drop(columns=["parent_id"], errors="ignore").reset_index(drop=True)
                for _,r in rdf.iterrows():
                    sub = r['uploaded_at'] if pd.notna(r.get('uploaded_at')) and str(r.get('uploaded_at')).strip() else r['report_date']
                    st.markdown(f"**Report No.** {int(r['cycle_no']) if pd.notna(r.get('cycle_no')) else ''}  \n**Period:** {r.get('window_start') or r['report_date']}  {r.get('window_end') or r['report_date']}  \n**Submitted:** {sub}  \n**Status:** {r['status']}  \n**Timing:** {_timing_status_label(r.get('timing_status')) if str(r.get('timing_status') or '').strip() else ''}")
                    with st.expander('Report details', expanded=False):
                        rendered_structured = _render_biweekly_structured_details(r.to_dict())
                        if not rendered_structured:
                            st.markdown(f"**Site Activities**\n\n{r.get('site_activities') or ''}")
                            st.markdown(f"**Reinforcement Observations**\n\n{r.get('reinforcement_observations') or ''}")
                            st.markdown(f"**Concrete / Test Observations**\n\n{r.get('concrete_observations') or ''}")
                        st.markdown(f"**HSE Observations**\n\n{r.get('hse_observations') or ''}")
                        st.markdown(f"**RFI / EI Notes**\n\n{r.get('rfi_notes') or ''}")
                        st.markdown(f"**General Remarks**\n\n{r.get('general_remarks') or ''}")
                    if str(r.get('review_note') or '').strip():
                        st.caption(f"Review note: {r.get('review_note')}")
                    if str(r.get('rejected_reason') or '').strip():
                        st.caption(f"Rejected reason: {r.get('rejected_reason')}")
                    btns = st.columns([1,1,1])
                    if str(r.get('file_path') or '').strip():
                        with btns[0]:
                            file_download_button('⬇️ Main File', r['file_path'], key=f"bw{r['id']}")
                    editable = _editable_status(r.get('status')) and (is_admin() or int(r.get('uploader_staff_id') or -1) == int(current_staff_id() or -999))
                    if editable:
                        with btns[1]:
                            if st.button('✏️ Edit', key=f"bwedit_{r['id']}"):
                                st.session_state[f"bw_edit_{pid}"] = int(r['id'])
                                st.rerun()
                    report_atts = atts_by_parent.get(int(r['id']), pd.DataFrame(columns=['id', 'file_path', 'caption', 'uploaded_at']))
                    pdf_path = _ensure_biweekly_pdf(r.to_dict(), project_name=f"{selected['code']}  {selected['name']}", attachments_df=report_atts)
                    with btns[2]:
                        render_pdf_preview_and_download(f"bw_{int(r['id'])}", pdf_path)
                    if is_admin():
                        with st.expander('🧾 Review / Approval', expanded=False):
                            review_note_val = st.text_area('Review note / revision instruction', value=str(r.get('review_note') or ''), key=f"bw_note_{r['id']}", help='Use this when sending a report back for correction or for internal review comments.')
                            reject_reason_val = st.text_area('Rejection reason', value=str(r.get('rejected_reason') or ''), key=f"bw_reject_{r['id']}", help='Required when rejecting this report.')
                            ra, rr, rj = st.columns(3)
                            with ra:
                                if r['status']!='APPROVED' and st.button('✅ Approve', key=f"bapp{r['id']}"):
                                    execute("UPDATE biweekly_reports SET review_note=?, reviewed_by_staff_id=?, reviewed_at=? WHERE id=?", (review_note_val or None, current_staff_id(), datetime.now().isoformat(timespec='seconds'), int(r['id'])))
                                    approve_biweekly_report(int(r['id']), current_staff_id())
                                    execute("UPDATE biweekly_reports SET approved_at=?, approved_by_staff_id=? WHERE id=?", (datetime.now().isoformat(timespec='seconds'), current_staff_id(), int(r['id'])))
                                    st.rerun()
                            with rr:
                                if r['status']!='NEEDS_REVISION' and st.button('🔁 Request Revision', key=f"brev{r['id']}"):
                                    execute("UPDATE biweekly_reports SET status='NEEDS_REVISION', review_note=?, reviewed_by_staff_id=?, reviewed_at=?, approved_at=NULL, approved_by_staff_id=NULL WHERE id=?",
                                            ((review_note_val or 'Please review and update this report before resubmitting.'), current_staff_id(), datetime.now().isoformat(timespec='seconds'), int(r['id'])))
                                    _sync_biweekly_points_for_report(int(r['id']))
                                    st.rerun()
                            with rj:
                                busy_key = f"reject_busy_{int(r['id'])}"
                                if r['status']!='REJECTED' and st.button('Reject', key=f"brej{r['id']}", disabled=st.session_state.get(busy_key, False)):
                                    if not str(reject_reason_val or '').strip():
                                        st.error("Rejection reason is required.")
                                    else:
                                        st.session_state[busy_key] = True
                                        try:
                                            execute(
                                                "UPDATE biweekly_reports SET status='REJECTED', rejected_reason=?, review_note=?, reviewed_by_staff_id=?, reviewed_at=?, approved_at=NULL, approved_by_staff_id=NULL WHERE id=?",
                                                (str(reject_reason_val).strip(), None, current_staff_id(), datetime.now().isoformat(timespec='seconds'), int(r['id']))
                                            )
                                            _sync_biweekly_points_for_report(int(r['id']))
                                        finally:
                                            st.session_state[busy_key] = False
                                        st.rerun()
                    _render_attachment_list('biweekly', int(r['id']), f"bwatt_{r['id']}")
                    st.markdown('---')
# ---------- Staff ----------
