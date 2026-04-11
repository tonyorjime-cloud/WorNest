import streamlit as st

def activity_section(title, key):
    st.markdown(f"## {title}")
    val = st.radio(f"Was {title.lower()} carried out?", ["Yes", "No"], horizontal=True, key=key)
    return val == "Yes"

def simple_module(title, pid, fields):
    if activity_section(title, f"{title}_{pid}"):
        values = []
        checks = []
        for f in fields:
            v = st.radio(f, ["Yes", "No"], key=f"{f}_{pid}")
            values.append(f"{f}: {v}")
            checks.append(v)
        return {"text":"\n".join(values),"checks":checks}
    return {"text":"Not Applicable","checks":[]}

def render_biweekly_form(pid, engineer_name):
    with st.form(f"biweekly_form_{pid}"):
        st.markdown("## 📝 Biweekly Site Report")

        concrete = simple_module("Concrete Works", pid, ["Slump Test Conducted?", "Cube Samples Taken?"])
        reinforcement = simple_module("Reinforcement Works", pid, ["Correct Bar Sizes?", "Spacing As Spec?", "Spacers Installed?"])
        formwork = simple_module("Formwork", pid, ["Proper Alignment?", "Adequate Bracing?", "Leakage Prevention OK?"])

        hse = st.text_area("HSE Observations")
        rfi = st.text_area("RFI / EI")
        remarks = st.text_area("General Remarks")

        submit = st.form_submit_button("Submit Report")

        if submit:
            checks = concrete["checks"] + reinforcement["checks"] + formwork["checks"]
            non_compliance = "No" in checks
            if non_compliance:
                remarks += "\n\n⚠️ NON-COMPLIANCE DETECTED"

            return {
                "site_activities": formwork["text"],
                "reinforcement_observations": reinforcement["text"],
                "concrete_observations": concrete["text"],
                "hse_observations": hse,
                "rfi_notes": rfi,
                "general_remarks": remarks,
                "non_compliance": non_compliance,
                "engineer": engineer_name
            }
    return None
