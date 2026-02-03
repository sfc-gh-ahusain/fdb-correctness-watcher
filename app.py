import streamlit as st
import pandas as pd
import requests
import os
import json
from jira_client import JiraClient, get_secret
from datetime import datetime, timedelta

try:
    from snowflake.snowpark.context import get_active_session
    IN_SNOWFLAKE = True
except ImportError:
    IN_SNOWFLAKE = False

try:
    from dotenv import load_dotenv, set_key
    load_dotenv()
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False

ENV_FILE = os.path.join(os.path.dirname(__file__), ".env") if HAS_DOTENV else None
WEBHOOKS_FILE = os.path.join(os.path.dirname(__file__), "webhooks.json")
WEBHOOKS_TABLE = "SNOWPUBLIC.STREAMLIT.FDB_WATCHER_WEBHOOKS"

def get_snowflake_session():
    if IN_SNOWFLAKE:
        try:
            return get_active_session()
        except:
            pass
    return None

def load_webhooks() -> dict:
    session = get_snowflake_session()
    if session:
        try:
            result = session.sql(f"SELECT name, url FROM {WEBHOOKS_TABLE}").collect()
            return {row["NAME"]: row["URL"] for row in result}
        except Exception as e:
            st.warning(f"Could not load webhooks from Snowflake: {e}")
            return {}
    if os.path.exists(WEBHOOKS_FILE):
        try:
            with open(WEBHOOKS_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {}

def save_webhooks(webhooks: dict):
    with open(WEBHOOKS_FILE, "w") as f:
        json.dump(webhooks, f, indent=2)

def add_webhook(name: str, url: str):
    session = get_snowflake_session()
    if session:
        try:
            session.sql(f"INSERT INTO {WEBHOOKS_TABLE} (name, url) VALUES (?, ?)", [name, url]).collect()
            return
        except Exception as e:
            st.warning(f"Could not save webhook to Snowflake: {e}")
    webhooks = load_webhooks()
    webhooks[name] = url
    save_webhooks(webhooks)

def delete_webhook(name: str):
    session = get_snowflake_session()
    if session:
        try:
            session.sql(f"DELETE FROM {WEBHOOKS_TABLE} WHERE name = ?", [name]).collect()
            return
        except Exception as e:
            st.warning(f"Could not delete webhook from Snowflake: {e}")
    webhooks = load_webhooks()
    if name in webhooks:
        del webhooks[name]
        save_webhooks(webhooks)

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
DEFAULT_DUPLICATE_THRESHOLD = 3

def generate_slack_message(df: pd.DataFrame, sla_days: int, exclude_under_sla: bool = False, totals: dict = None, duplicate_threshold: int = DEFAULT_DUPLICATE_THRESHOLD) -> str:
    lines = []
    lines.append("*📊 FDB Correctness SLA Report*")
    lines.append("")
    
    lines.append("*Legend:*")
    lines.append("• 🟢 Under SLA (within limit)")
    lines.append(f"• 🟡 Warning (>{int(sla_days * 0.8)}d of {sla_days}d SLA)")
    lines.append("• 🔴 Breached (> 14 days, over SLA)")
    lines.append(f"• 📢 Noisy (>{duplicate_threshold} duplicates)")
    lines.append("")
    
    total = len(df)
    over_sla = len(df[df["sla_status"] == "over"])
    warning_threshold = sla_days * 0.8
    warning_count = len(df[(df["days_open"] > warning_threshold) & (df["sla_status"] == "under")])
    under_sla = total - over_sla - warning_count
    total_duplicates = df["duplicate_count"].sum() if "duplicate_count" in df.columns else 0
    
    lines.append("*Summary:*")
    if totals and totals.get("total") != total:
        lines.append(f"• Total Tickets: {total} (of {totals['total']})")
        lines.append(f"• 🔴 SLA Breached: {over_sla} (of {totals['over_sla']})")
        lines.append(f"• 🟡 SLA Warning (>{int(warning_threshold)}d): {warning_count}")
        lines.append(f"• 🟢 SLA OK: {under_sla} (of {totals['under_sla']})")
        lines.append(f"• 📋 Total Duplicates: {int(total_duplicates)}")
    else:
        lines.append(f"• Total Tickets: {total}")
        lines.append(f"• 🔴 SLA Breached: {over_sla}")
        lines.append(f"• 🟡 SLA Warning (>{int(warning_threshold)}d): {warning_count}")
        lines.append(f"• 🟢 SLA OK: {under_sla}")
        lines.append(f"• 📋 Total Duplicates: {int(total_duplicates)}")
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
            duplicate_count = row.get("duplicate_count", 0)
            
            if sla_status == "over":
                issue_indicator = "🔴"
            elif days_open > warning_threshold:
                issue_indicator = "🟡"
            else:
                issue_indicator = "🟢"
            
            is_noisy = duplicate_count > duplicate_threshold
            dup_str = f", 📢 {duplicate_count} dups" if is_noisy else (f", {duplicate_count} dups" if duplicate_count > 0 else "")
            noisy_prefix = "*" if is_noisy else ""
            noisy_suffix = "* ⚠️" if is_noisy else ""
            lines.append(f"    • {issue_indicator} {noisy_prefix}<{url}|{key}>{noisy_suffix} [{priority}] {days_open}d old, upd {days_since_update}d ago{dup_str}")
        
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
        
        st.subheader(":material/content_copy: Noise Threshold")
        duplicate_threshold = st.number_input("Duplicate Count Threshold", min_value=1, value=DEFAULT_DUPLICATE_THRESHOLD, help="Issues with more duplicates than this are highlighted as noisy")
        
        st.divider()
        
        st.subheader(":material/webhook: Slack Webhooks")
        webhooks = load_webhooks()
        
        if webhooks:
            st.caption(f"{len(webhooks)} webhook(s) configured")
            for name in list(webhooks.keys()):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.text(f"• {name}")
                with col2:
                    if st.button("🗑️", key=f"del_{name}", help=f"Delete {name}"):
                        delete_webhook(name)
                        st.rerun()
        
        with st.expander("Add Webhook", expanded=len(webhooks) == 0):
            with st.form("add_webhook_form", clear_on_submit=True):
                new_name = st.text_input("Name", placeholder="e.g., #fdb-alerts")
                new_url = st.text_input("URL", placeholder="https://hooks.slack.com/services/...", type="password")
                if st.form_submit_button(":material/add: Add Webhook", use_container_width=True):
                    if new_name and new_url:
                        add_webhook(new_name, new_url)
                        st.success(f"Added '{new_name}'")
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
        st.caption(f"Debug: Loaded {len(issues) if issues else 0} issues")
    except Exception as e:
        st.error(f"Failed to fetch JIRA issues: {e}")
        import traceback
        st.code(traceback.format_exc())
        return
    
    if not issues:
        st.warning("No issues found matching the criteria")
        st.code(f"JQL: {jql}")
        return
    
    df = pd.DataFrame(issues)
    df = apply_sla_rules(df, sla_days)
    df["is_noisy"] = df["duplicate_count"] > duplicate_threshold
    
    with st.sidebar:
        st.subheader(":material/filter_alt: Filters")
        areas = sorted(df["area"].unique().tolist())
        selected_area = st.selectbox("Area", ["All"] + areas)
        
        statuses = sorted(df["status"].unique().tolist())
        default_statuses = [s for s in ["To Do", "Triaged", "IN PROGRESS"] if s in statuses]
        selected_statuses = st.multiselect("Status", statuses, default=default_statuses)
    
    if selected_statuses:
        df = df[df["status"].isin(selected_statuses)]
    
    total_all = len(df)
    total_under_sla_all = len(df[df["sla_status"] == "under"])
    total_over_sla_all = len(df[df["sla_status"] == "over"])
    
    if selected_area != "All":
        df = df[df["area"] == selected_area]
    
    status_counts = df["status"].value_counts().to_dict()
    sla_violations = len(df[df["sla_status"] == "over"])
    
    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        label = f"{len(df)} (of {total_all})" if selected_area != "All" else str(len(df))
        st.metric("Total Issues", label)
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
            label = f"{under_sla_count} (of {total_under_sla_all})" if selected_area != "All" else str(under_sla_count)
            st.metric("🟢 Under SLA", label)
        with sla_col2:
            label = f"{over_sla_count} (of {total_over_sla_all})" if selected_area != "All" else str(over_sla_count)
            st.metric("🔴 Over SLA", label)
        
        st.markdown("##### 📊 Participant Statistics")
        participant_stats = df.groupby("assignee").agg(
            total_assigned=("key", "count"),
            sla_violations=("sla_status", lambda x: (x == "over").sum()),
            oldest_days=("days_open", "max"),
            to_do=("status", lambda x: (x == "To Do").sum()),
            in_progress=("status", lambda x: ((x == "In Progress") | (x == "IN PROGRESS")).sum()),
            total_duplicates=("duplicate_count", "sum"),
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
            participant_stats[["assignee", "total_assigned", "to_do", "in_progress", "sla_display", "age_display", "total_duplicates"]],
            column_config={
                "assignee": "Participant",
                "total_assigned": st.column_config.NumberColumn("Total", format="%d"),
                "to_do": st.column_config.NumberColumn("To Do", format="%d"),
                "in_progress": st.column_config.NumberColumn("In Progress", format="%d"),
                "sla_display": "SLA Violations",
                "age_display": "Oldest (Days)",
                "total_duplicates": st.column_config.NumberColumn("Duplicates", format="%d", help="Total duplicate issues linked to this participant's tickets"),
            },
            hide_index=True,
            use_container_width=True
        )
        
        noisy_tickets = df[df["duplicate_count"] > 0].sort_values("duplicate_count", ascending=False)
        if len(noisy_tickets) > 0:
            st.markdown("##### 🔊 Noisiest Tickets (by duplicate count)")
            st.caption("Higher duplicate count = more incidents from this issue in correctness runs")
            st.dataframe(
                noisy_tickets[["key", "summary", "assignee", "status", "days_open", "duplicate_count", "url"]].head(10),
                column_config={
                    "key": "Issue",
                    "summary": "Summary",
                    "assignee": "Assignee",
                    "status": "Status",
                    "days_open": st.column_config.NumberColumn("Days Open", format="%d"),
                    "duplicate_count": st.column_config.NumberColumn("Duplicates", format="%d"),
                    "url": st.column_config.LinkColumn("Link", display_text="Open")
                },
                hide_index=True,
                use_container_width=True
            )
    
    st.divider()
    
    tab1, tab2, tab3 = st.tabs([":material/checklist: Status Overview", ":material/group: By Participant", ":material/warning: SLA Report"])
    
    with tab1:
        render_status_view(df, duplicate_threshold)
    
    with tab2:
        render_participant_view(df, duplicate_threshold)
    
    with tab3:
        render_sla_report(df, duplicate_threshold)
    
    st.divider()
    
    with st.expander(":material/share: **Generate Slack Message**", expanded=False):
        exclude_under_sla = st.checkbox("Exclude JIRAs under SLA (show only violations)", value=False)
        totals = {"total": total_all, "under_sla": total_under_sla_all, "over_sla": total_over_sla_all}
        noisy_count = len(df[df["is_noisy"]])
        is_filtered = len(df) != total_all
        slack_msg = generate_slack_message(df, sla_days, exclude_under_sla, totals if is_filtered else None, duplicate_threshold)
        
        webhooks = load_webhooks()
        if webhooks:
            webhook_names = list(webhooks.keys())
            col1, col2 = st.columns([2, 1])
            with col1:
                selected_webhook = st.selectbox("Send to", webhook_names, key="webhook_select")
            with col2:
                send_btn = st.button(":material/send: Send", type="primary", use_container_width=True)
            
            if send_btn and selected_webhook:
                try:
                    response = requests.post(
                        webhooks[selected_webhook],
                        json={"text": slack_msg, "mrkdwn": True},
                        headers={"Content-Type": "application/json"}
                    )
                    if response.status_code == 200:
                        st.success(f"✅ Sent to {selected_webhook}!")
                    else:
                        st.error(f"Failed to send: {response.text}")
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Configure Slack webhook(s) in the sidebar to enable sending.")
        
        st.divider()
        st.subheader("Preview")
        st.text_area("Message preview (select all & copy):", value=slack_msg, height=400)
        st.caption("💡 Use webhook to send with clickable JIRA links, or copy-paste (URLs will auto-link).")

def render_status_view(df: pd.DataFrame, duplicate_threshold: int = DEFAULT_DUPLICATE_THRESHOLD):
    st.subheader("Issues by Status")
    
    statuses = sorted(df["status"].unique().tolist())
    
    for status in statuses:
        status_df = df[df["status"] == status]
        if len(status_df) > 0:
            with st.expander(f"{status} ({len(status_df)} issues)", expanded=True):
                display_df = status_df[["priority", "assignee", "days_open", "sla_status", "duplicate_count", "key", "url"]].copy().reset_index(drop=True)
                display_df["sla_status"] = display_df["sla_status"].apply(lambda x: "🟢" if x == "under" else "🔴" if x == "over" else "-")
                display_df["dup_display"] = display_df["duplicate_count"].apply(lambda x: f"📢 {x}" if x > duplicate_threshold else str(x))
                
                final_df = display_df[["priority", "assignee", "days_open", "sla_status", "dup_display", "url"]].copy()
                dup_counts = display_df["duplicate_count"].values
                
                def highlight_noisy(row):
                    idx = row.name
                    if idx < len(dup_counts) and dup_counts[idx] > duplicate_threshold:
                        return ["background-color: #fff3cd"] * len(row)
                    return [""] * len(row)
                
                styled_df = final_df.style.apply(highlight_noisy, axis=1)
                st.dataframe(
                    styled_df,
                    column_config={
                        "priority": "Priority",
                        "assignee": "Assignee",
                        "days_open": st.column_config.NumberColumn("Days Open", format="%d"),
                        "sla_status": "SLA",
                        "dup_display": st.column_config.TextColumn("Duplicates", help="Number of duplicate issues linked. 📢 = noisy"),
                        "url": st.column_config.LinkColumn("Issue", display_text=r".*browse/(.*)")
                    },
                    hide_index=True,
                    use_container_width=True
                )

def render_participant_view(df: pd.DataFrame, duplicate_threshold: int = DEFAULT_DUPLICATE_THRESHOLD):
    st.subheader("Issues by Participant")
    
    participants = sorted(df["assignee"].unique().tolist())
    
    selected = st.selectbox("Select Participant", ["All"] + participants)
    
    if selected == "All":
        participant_summary = df.groupby("assignee").agg(
            total=("key", "count"),
            todo=("status", lambda x: (x == "To Do").sum()),
            in_progress=("status", lambda x: (x == "In Progress").sum()),
            sla_violations=("sla_status", lambda x: (x == "over").sum()),
            total_duplicates=("duplicate_count", "sum")
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
                "sla_indicator": "Status",
                "total_duplicates": st.column_config.NumberColumn("Duplicates", format="%d", help="Total duplicate issues (indicates noise level)")
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        participant_df = df[df["assignee"] == selected]
        render_participant_detail(participant_df, selected, duplicate_threshold)

def render_participant_detail(df: pd.DataFrame, participant: str, duplicate_threshold: int = DEFAULT_DUPLICATE_THRESHOLD):
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
        display_issues_table(under_sla_df, duplicate_threshold)
    
    if len(over_sla_df) > 0:
        st.markdown("#### 🔴 Over SLA")
        display_issues_table(over_sla_df, duplicate_threshold)

def render_sla_report(df: pd.DataFrame, duplicate_threshold: int = DEFAULT_DUPLICATE_THRESHOLD):
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
            display_issues_table(under_sla, duplicate_threshold)
        else:
            st.info("No issues under SLA")
    
    with col2:
        st.markdown("### 🔴 Over SLA (Violations)")
        over_sla = critical_high_df[critical_high_df["sla_status"] == "over"]
        if len(over_sla) > 0:
            display_issues_table(over_sla, duplicate_threshold)
        else:
            st.success("No SLA violations!")

def display_issues_table(df: pd.DataFrame, duplicate_threshold: int = DEFAULT_DUPLICATE_THRESHOLD):
    display_df = df[["key", "summary", "priority", "status", "assignee", "days_open", "sla_limit", "duplicate_count"]].copy().reset_index(drop=True)
    display_df["remaining"] = display_df.apply(lambda row: row["sla_limit"] - row["days_open"] if row["sla_limit"] else None, axis=1)
    display_df["dup_display"] = display_df["duplicate_count"].apply(lambda x: f"📢 {x}" if x > duplicate_threshold else str(x))
    
    final_df = display_df[["key", "summary", "priority", "status", "assignee", "days_open", "remaining", "dup_display"]].copy()
    dup_counts = display_df["duplicate_count"].values
    
    def highlight_noisy(row):
        idx = row.name
        if idx < len(dup_counts) and dup_counts[idx] > duplicate_threshold:
            return ["background-color: #fff3cd"] * len(row)
        return [""] * len(row)
    
    styled_df = final_df.style.apply(highlight_noisy, axis=1)
    
    st.dataframe(
        styled_df,
        column_config={
            "key": "Issue",
            "summary": "Summary",
            "priority": "Priority",
            "status": "Status",
            "assignee": "Assignee",
            "days_open": st.column_config.NumberColumn("Days Open", format="%d"),
            "remaining": st.column_config.NumberColumn("Days Remaining", format="%d"),
            "dup_display": st.column_config.TextColumn("Duplicates", help="Number of duplicate issues linked. 📢 = noisy")
        },
        hide_index=True,
        use_container_width=True
    )

if __name__ == "__main__":
    main()
