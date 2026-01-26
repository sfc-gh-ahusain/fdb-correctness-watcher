# FDB Correctness Watcher

A Streamlit dashboard for monitoring FoundationDB test failure tickets in JIRA with SLA tracking and Slack integration.

## Features

- **Multiple Views**: Pre-configured JQL queries for common scenarios (Open Tickets, By Owner, Unassigned, etc.)
- **Custom JQL**: Write your own JIRA queries
- **SLA Tracking**: Configurable SLA threshold with visual indicators (green/yellow/red)
- **Participant Statistics**: Per-assignee breakdown with violation counts
- **Slack Integration**: Send formatted SLA reports directly to Slack via webhook
- **Filtering**: Filter by area, status, and priority

## Views Available

| View | Description |
|------|-------------|
| All Open Tickets | All non-closed TestFailure tickets |
| All Main Tickets | Tickets labeled FDB_MAIN |
| Tickets By Owner | Grouped by assignee |
| Unassigned Tickets | Tickets without assignee |
| Open Tickets - 25.x/26.0/Main | Version-specific views |
| PR Canary failures | Last 7 days of PR canary issues |
| Tickets Closed Past 7 Days | Recently resolved tickets |
| Custom JQL | Your own query |

## Setup

### Prerequisites

- Python 3.9+
- JIRA API access (Atlassian account)

### Installation

```bash
git clone https://github.com/sfc-gh-ahusain/fdb-correctness-watcher.git
cd fdb-correctness-watcher
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Create a `.env` file:

```env
JIRA_BASE_URL=https://your-org.atlassian.net
JIRA_EMAIL=your-email@example.com
JIRA_API_TOKEN=your-api-token
JIRA_PROJECT_KEY=FDBCORE
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...  # Optional
```

To get a JIRA API token:
1. Go to https://id.atlassian.com/manage-profile/security/api-tokens
2. Create a new token

To get a Slack webhook URL:
1. Go to https://api.slack.com/apps
2. Create an app > Incoming Webhooks > Add to channel

## Usage

### Start the app

```bash
source venv/bin/activate
streamlit run app.py --server.port 8504
```

Or use the alias (if configured):

```bash
start_fdb_correctness  # Start on port 8504
stop_fdb_correctness   # Stop the app
```

### App Interface

1. **Sidebar**: Select view, configure SLA days, set up Slack webhook
2. **Quick Summary**: Overview of SLA status and participant statistics
3. **Tabs**: 
   - Status Overview: Issues grouped by status
   - By Participant: Issues grouped by assignee
   - SLA Report: Under/Over SLA breakdown
4. **Generate Slack Message**: Create and send formatted reports to Slack

## SLA Indicators

| Indicator | Meaning |
|-----------|---------|
| 🟢 | Under SLA / OK |
| 🟡 | Warning (>80% of SLA threshold) |
| 🔴 | SLA Breached |

## License

Internal use only.
