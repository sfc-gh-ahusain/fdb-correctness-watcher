import streamlit as st
import pandas as pd
import requests
import os
from jira_client import JiraClient
from datetime import datetime, timedelta
from dotenv import load_dotenv, set_key

load_dotenv()

ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")

def get_slack_webhook():
    return os.getenv("SLACK_WEBHOOK_URL", "")

def save_slack_webhook(url: str):
    set_key(ENV_FILE, "SLACK_WEBHOOK_URL", url)
    os.environ["SLACK_WEBHOOK_URL"] = url

st.set_page_config(page_title="FDB Correctness Watcher", page_icon=":material/radar:", layout="wide")

VIEWS = {
    "-- Select a View --": None,
    "Custom JQL": "CUSTOM",
    "All Open Tickets": 'project = FDBCORE AND type = TestFailure AND status NOT IN ("Won\'t Do", "Done") AND (labels NOT IN ("FDB_BUILDCOP_IGNORE") OR labels IS EMPTY) ORDER BY priority DESC, created DESC',
    "All Main Tickets": 'project = FDBCORE AND type = TestFailure AND labels = "FDB_MAIN" AND status NOT IN ("Won\'t Do") ORDER BY priority DESC, created DESC',
    "All Tickets (No Duplicates)": 'project = FDBCORE AND type = TestFailure AND status NOT IN ("Won\'t Do") AND (labels NOT IN ("FDB_BUILDCOP_IGNORE", "duplicate") OR labels IS EMPTY) ORDER BY priority DESC, created DESC',
    "Awaiting Cherry-Pick": 'project = FDBCORE AND type = TestFailure AND status = "Awaiting Cherry-Pick" ORDER BY priority DESC, created DESC',
    "Code Probes": 'project = FDBCORE AND type = TestFailure AND labels = "code_probes" ORDER BY priority DESC, created DESC',
    "Open Tickets - 25.5": 'project = FDBCORE AND type = TestFailure AND labels = "FDB_25.5" AND status NOT IN ("Won\'t Do", "Done") ORDER BY priority DESC, created DESC',
    "Open Tickets - 25.6": 'project = FDBCORE AND type = TestFailure AND labels = "FDB_25.6" AND status NOT IN ("Won\'t Do", "Done") ORDER BY priority DESC, created DESC',
    "Open Tickets - 25.7": 'project = FDBCORE AND type = TestFailure AND labels = "FDB_25.7" AND status NOT IN ("Won\'t Do", "Done") ORDER BY priority DESC, created DESC',
    "Open Tickets - 26.0": 'project = FDBCORE AND type = TestFailure AND labels = "FDB_26.0" AND status NOT IN ("Won\'t Do", "Done") ORDER BY priority DESC, created DESC',
    "Open Tickets - Main": 'project = FDBCORE AND type = TestFailure AND labels = "FDB_MAIN" AND status NOT IN ("Won\'t Do", "Done") ORDER BY priority DESC, created DESC',
    "PR Canary failures last week": f'project = FDBCORE AND type = TestFailure AND labels = "PR_Canary" AND created >= -{7}d ORDER BY created DESC',
    "Tickets By Owner": 'project = FDBCORE AND type = TestFailure AND assignee IS NOT EMPTY AND status NOT IN ("Won\'t Do", "Done", "DUPLICATE") AND (labels NOT IN ("FDB_BUILDCOP_IGNORE") OR labels IS EMPTY) ORDER BY assignee ASC, priority DESC',
    "Tickets Closed Past 7 Days": f'project = FDBCORE AND type = TestFailure AND status IN ("Done", "Won\'t Do") AND updated >= -{7}d ORDER BY updated DESC',
    "Unassigned Tickets": 'project = FDBCORE AND type = TestFailure AND assignee IS EMPTY AND status NOT IN ("Won\'t Do", "Done") ORDER BY priority DESC, created DESC',
}

DEFAULT_VIEW = "-- Select a View --"

DEFAULT_SLA_DAYS = 14

def generate_slack_message(df: pd.DataFrame, sla_days: int, exclude_under_sla: bool = False) -> str:
    lines = []
    lines.append("*📊 FDB Correctness SLA Report*")
    lines.append("")
    
    lines.append("*Legend:*")
    lines.append("• 🟢 Under SLA (within limit)")
    lines.append(f"• 🟡 Warning (>{int(sla_days * 0.8)}d of {sla_days}d SLA)")
    lines.append("• 🔴 Breached (over SLA)")
    lines.append("")
    
    total = len(df)
    over_sla = len(df[df["sla_status"] == "over"])
    warning_threshold = sla_days * 0.8
    warning_count = len(df[(df["days_open"] > warning_threshold) & (df["sla_status"] == "under")])
    under_sla = total - over_sla - warning_count
    
    lines.append("*Summary:*")
    lines.append(f"• Total Tickets: {total}")
    lines.append(f"• 🔴 SLA Breached: {over_sla}")
    lines.append(f"• 🟡 SLA Warning (>{int(warning_threshold)}d): {warning_count}")
    lines.append(f"• 🟢 SLA OK: {under_sla}")
    lines.append("")
    
    work_df = df.copy()
    if exclude_under_sla:
        work_df = work_df[(work_df["sla_status"] == "over") | (work_df["days_open"] > warning_threshold)]
    
    if len(work_df) == 0:
        lines.append("_No issues to report._")
        return "\n".join(lines)
    
    lines.append("*Per Participant:*")
    participants = sorted(work_df["assignee"].unique().tolist())
    
    for participant in participants:
        p_df = work_df[work_df["assignee"] == participant]
        breached = len(p_df[p_df["sla_status"] == "over"])
        warnings = len(p_df[(p_df["days_open"] > warning_threshold) & (p_df["sla_status"] == "under")])
        
        if breached > 0:
            indicator = "🔴"
        elif warnings > 0:
            indicator = "🟡"
        else:
            indicator = "🟢"
        
        status_parts = []
        if breached > 0:
            status_parts.append(f"{breached} breached")
        if warnings > 0:
            status_parts.append(f"{warnings} warning")
        ok_count = len(p_df) - breached - warnings
        if ok_count > 0 and not exclude_under_sla:
            status_parts.append(f"{ok_count} ok")
        
        lines.append(f"{indicator} *@{participant}* ({len(p_df)} tickets, {', '.join(status_parts)}):")
        
        for _, row in p_df.iterrows():
            key = row["key"]
            url = row["url"]
            priority = row["priority"]
            days_open = row["days_open"]
            days_since_update = row.get("days_since_update", 0)
            sla_status = row["sla_status"]
            
            if sla_status == "over":
                issue_indicator = "🔴"
            elif days_open > warning_threshold:
                issue_indicator = "🟡"
            else:
                issue_indicator = "🟢"
            
            lines.append(f"    • {issue_indicator} <{url}|{key}> [{priority}] {days_open}d old, upd {days_since_update}d ago")
        
        lines.append("")
    
    return "\n".join(lines)

def apply_sla_rules(df: pd.DataFrame, sla_days: int) -> pd.DataFrame:
    df["sla_limit"] = sla_days
    df["sla_status"] = df["days_open"].apply(lambda x: "under" if x <= sla_days else "over")
    return df

@st.cache_data(ttl=300)
def load_issues(jql: str):
    client = JiraClient()
    return client.get_fdb_storage_issues(custom_jql=jql)

def main():
    if "sidebar_collapsed" not in st.session_state:
        st.session_state.sidebar_collapsed = False
    
    with st.sidebar:
        st.title(":material/bug_report: Config")
        
        st.subheader(":material/view_list: View")
        selected_view = st.selectbox(
            "Select View",
            options=list(VIEWS.keys()),
            index=list(VIEWS.keys()).index(DEFAULT_VIEW),
            label_visibility="collapsed",
            key="view_selector"
        )
        
        jql = VIEWS[selected_view]
        
        st.subheader(":material/tune: Filters")
        
        is_custom = selected_view == "Custom JQL"
        with st.expander("Custom JQL", expanded=is_custom):
            custom_jql = st.text_area(
                "JQL Query", 
                value="" if is_custom else (jql or ""), 
                height=100, 
                label_visibility="collapsed",
                disabled=not is_custom,
                placeholder="Enter your JQL query here...",
                key="custom_jql_input"
            )
            if is_custom and custom_jql:
                jql = custom_jql
            elif is_custom:
                jql = None
        
        st.subheader(":material/timer: SLA Rule")
        sla_days = st.number_input("Days Open Threshold", min_value=1, value=DEFAULT_SLA_DAYS, help="Issues open longer than this are flagged as SLA violations")
        
        st.divider()
        
        st.subheader(":material/webhook: Slack Integration")
        saved_webhook = get_slack_webhook()
        webhook_url = st.text_input(
            "Webhook URL",
            value=saved_webhook,
            type="password",
            placeholder="https://hooks.slack.com/services/...",
            help="Get a webhook URL from your Slack workspace settings"
        )
        if webhook_url != saved_webhook:
            if st.button(":material/save: Save Webhook", use_container_width=True):
                save_slack_webhook(webhook_url)
                st.success("Webhook saved!")
                st.rerun()
        
        st.divider()
        
        if st.button(":material/refresh: Refresh Data", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    st.title(":material/radar: FDB Correctness Watcher")
    st.caption(f"View: **{selected_view}**")
    
    if jql is None:
        st.info(":material/arrow_back: Select a view from the sidebar to load issues")
        return
    
    try:
        issues = load_issues(jql)
    except Exception as e:
        st.error(f"Failed to fetch JIRA issues: {e}")
        return
    
    if not issues:
        st.warning("No issues found matching the criteria")
        return
    
    df = pd.DataFrame(issues)
    df = apply_sla_rules(df, sla_days)
    
    with st.sidebar:
        st.subheader(":material/filter_alt: Filters")
        areas = sorted(df["area"].unique().tolist())
        selected_area = st.selectbox("Area", ["All"] + areas)
        
        statuses = sorted(df["status"].unique().tolist())
        default_statuses = [s for s in ["To Do", "Triaged", "IN PROGRESS"] if s in statuses]
        selected_statuses = st.multiselect("Status", statuses, default=default_statuses)
    
    if selected_area != "All":
        df = df[df["area"] == selected_area]
    
    if selected_statuses:
        df = df[df["status"].isin(selected_statuses)]
    
    status_counts = df["status"].value_counts().to_dict()
    sla_violations = len(df[df["sla_status"] == "over"])
    
    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        st.metric("Total Issues", len(df))
    with col2:
        if len(status_counts) > 0:
            status_cols = st.columns(len(status_counts))
            for i, (status, count) in enumerate(status_counts.items()):
                with status_cols[i]:
                    st.metric(status, count)
    with col3:
        st.metric("SLA Violations", sla_violations, delta=None if sla_violations == 0 else f"-{sla_violations}", delta_color="inverse")
    
    st.divider()
    
    st.markdown("""
    <div style="background-color: #f0f2f6; padding: 10px 14px; border-radius: 6px; margin-bottom: 12px; font-size: 1.0em;">
        <strong>Legend:</strong>&nbsp;&nbsp;
        🟢 Good (no violations / within SLA) &nbsp;&nbsp;|&nbsp;&nbsp;
        🟡 Warning (approaching limit) &nbsp;&nbsp;|&nbsp;&nbsp;
        🔴 Critical (violations / over SLA)
    </div>
    """, unsafe_allow_html=True)
    
    with st.expander(":material/summarize: **Quick Summary**", expanded=True):
        under_sla_count = len(df[df["sla_status"] == "under"])
        over_sla_count = len(df[df["sla_status"] == "over"])
        
        sla_col1, sla_col2 = st.columns(2)
        with sla_col1:
            st.metric("🟢 Under SLA", under_sla_count)
        with sla_col2:
            st.metric("🔴 Over SLA", over_sla_count)
        
        st.markdown("##### 📊 Participant Statistics")
        participant_stats = df.groupby("assignee").agg(
            total_assigned=("key", "count"),
            sla_violations=("sla_status", lambda x: (x == "over").sum()),
            oldest_days=("days_open", "max"),
            to_do=("status", lambda x: (x == "To Do").sum()),
            in_progress=("status", lambda x: ((x == "In Progress") | (x == "IN PROGRESS")).sum()),
        ).reset_index()
        
        participant_stats = participant_stats.sort_values("total_assigned", ascending=False)
        
        participant_stats["sla_indicator"] = participant_stats["sla_violations"].apply(
            lambda x: "🟢" if x == 0 else "🔴"
        )
        participant_stats["age_indicator"] = participant_stats["oldest_days"].apply(
            lambda x: "🟢" if x <= sla_days else ("🟡" if x <= sla_days * 2 else "🔴")
        )
        participant_stats["sla_display"] = participant_stats.apply(
            lambda r: f"{r['sla_indicator']} {r['sla_violations']}", axis=1
        )
        participant_stats["age_display"] = participant_stats.apply(
            lambda r: f"{r['age_indicator']} {r['oldest_days']}", axis=1
        )
        
        st.dataframe(
            participant_stats[["assignee", "total_assigned", "to_do", "in_progress", "sla_display", "age_display"]],
            column_config={
                "assignee": "Participant",
                "total_assigned": st.column_config.NumberColumn("Total", format="%d"),
                "to_do": st.column_config.NumberColumn("To Do", format="%d"),
                "in_progress": st.column_config.NumberColumn("In Progress", format="%d"),
                "sla_display": "SLA Violations",
                "age_display": "Oldest (Days)",
            },
            hide_index=True,
            use_container_width=True
        )
    
    st.divider()
    
    tab1, tab2, tab3 = st.tabs([":material/checklist: Status Overview", ":material/group: By Participant", ":material/warning: SLA Report"])
    
    with tab1:
        render_status_view(df)
    
    with tab2:
        render_participant_view(df)
    
    with tab3:
        render_sla_report(df)
    
    st.divider()
    
    with st.expander(":material/share: **Generate Slack Message**", expanded=False):
        exclude_under_sla = st.checkbox("Exclude JIRAs under SLA (show only violations)", value=False)
        slack_msg = generate_slack_message(df, sla_days, exclude_under_sla)
        
        saved_webhook = get_slack_webhook()
        if saved_webhook:
            col1, col2 = st.columns([1, 3])
            with col1:
                send_btn = st.button(":material/send: Send to Slack", type="primary")
            with col2:
                st.caption("Webhook configured ✓")
            
            if send_btn:
                try:
                    response = requests.post(
                        saved_webhook,
                        json={"text": slack_msg, "mrkdwn": True},
                        headers={"Content-Type": "application/json"}
                    )
                    if response.status_code == 200:
                        st.success("✅ Message sent to Slack!")
                    else:
                        st.error(f"Failed to send: {response.text}")
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Configure Slack webhook in the sidebar to enable sending.")
        
        st.divider()
        st.subheader("Preview")
        st.text_area("Message preview (select all & copy):", value=slack_msg, height=400)
        st.caption("💡 Use webhook to send with clickable JIRA links, or copy-paste (URLs will auto-link).")

def render_status_view(df: pd.DataFrame):
    st.subheader("Issues by Status")
    
    statuses = sorted(df["status"].unique().tolist())
    
    for status in statuses:
        status_df = df[df["status"] == status]
        if len(status_df) > 0:
            with st.expander(f"{status} ({len(status_df)} issues)", expanded=True):
                display_df = status_df[["priority", "assignee", "days_open", "sla_status", "key", "url"]].copy()
                display_df["sla_status"] = display_df["sla_status"].apply(lambda x: "🟢" if x == "under" else "🔴" if x == "over" else "-")
                st.dataframe(
                    display_df[["priority", "assignee", "days_open", "sla_status", "url"]],
                    column_config={
                        "priority": "Priority",
                        "assignee": "Assignee",
                        "days_open": st.column_config.NumberColumn("Days Open", format="%d"),
                        "sla_status": "SLA",
                        "url": st.column_config.LinkColumn("Issue", display_text=r".*browse/(.*)")
                    },
                    hide_index=True,
                    use_container_width=True
                )

def render_participant_view(df: pd.DataFrame):
    st.subheader("Issues by Participant")
    
    participants = sorted(df["assignee"].unique().tolist())
    
    selected = st.selectbox("Select Participant", ["All"] + participants)
    
    if selected == "All":
        participant_summary = df.groupby("assignee").agg(
            total=("key", "count"),
            todo=("status", lambda x: (x == "To Do").sum()),
            in_progress=("status", lambda x: (x == "In Progress").sum()),
            sla_violations=("sla_status", lambda x: (x == "over").sum())
        ).reset_index()
        
        participant_summary["sla_indicator"] = participant_summary.apply(
            lambda row: "🟢" if row["sla_violations"] == 0 else "🔴", axis=1
        )
        
        st.dataframe(
            participant_summary,
            column_config={
                "assignee": "Participant",
                "total": "Total",
                "todo": "To Do",
                "in_progress": "In Progress",
                "sla_violations": "SLA Violations",
                "sla_indicator": "Status"
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        participant_df = df[df["assignee"] == selected]
        render_participant_detail(participant_df, selected)

def render_participant_detail(df: pd.DataFrame, participant: str):
    st.markdown(f"### {participant}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Issues", len(df))
    with col2:
        under_sla = len(df[df["sla_status"] == "under"])
        st.metric("Under SLA", under_sla)
    with col3:
        over_sla = len(df[df["sla_status"] == "over"])
        st.metric("Over SLA", over_sla, delta=None if over_sla == 0 else f"-{over_sla}", delta_color="inverse")
    
    under_sla_df = df[df["sla_status"] == "under"]
    over_sla_df = df[df["sla_status"] == "over"]
    
    if len(under_sla_df) > 0:
        st.markdown("#### 🟢 Under SLA")
        display_issues_table(under_sla_df)
    
    if len(over_sla_df) > 0:
        st.markdown("#### 🔴 Over SLA")
        display_issues_table(over_sla_df)

def render_sla_report(df: pd.DataFrame):
    st.subheader("SLA Report")
    
    priorities = df["priority"].unique().tolist()
    selected_priorities = st.multiselect("Filter by Priority", priorities, default=["Critical", "High"] if "Critical" in priorities else priorities[:2])
    
    if selected_priorities:
        critical_high_df = df[df["priority"].isin(selected_priorities)]
    else:
        critical_high_df = df
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🟢 Under SLA")
        under_sla = critical_high_df[critical_high_df["sla_status"] == "under"]
        if len(under_sla) > 0:
            display_issues_table(under_sla)
        else:
            st.info("No issues under SLA")
    
    with col2:
        st.markdown("### 🔴 Over SLA (Violations)")
        over_sla = critical_high_df[critical_high_df["sla_status"] == "over"]
        if len(over_sla) > 0:
            display_issues_table(over_sla)
        else:
            st.success("No SLA violations!")

def display_issues_table(df: pd.DataFrame):
    display_df = df[["key", "summary", "priority", "status", "assignee", "days_open", "sla_limit"]].copy()
    display_df["key"] = display_df.apply(lambda row: row["key"], axis=1)
    display_df["remaining"] = display_df.apply(lambda row: row["sla_limit"] - row["days_open"] if row["sla_limit"] else None, axis=1)
    
    st.dataframe(
        display_df[["key", "summary", "priority", "status", "assignee", "days_open", "remaining"]],
        column_config={
            "key": "Issue",
            "summary": "Summary",
            "priority": "Priority",
            "status": "Status",
            "assignee": "Assignee",
            "days_open": st.column_config.NumberColumn("Days Open", format="%d"),
            "remaining": st.column_config.NumberColumn("Days Remaining", format="%d")
        },
        hide_index=True,
        use_container_width=True
    )

if __name__ == "__main__":
    main()
