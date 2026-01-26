import os
import requests
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class JiraClient:
    def __init__(self):
        self.base_url = os.getenv("JIRA_BASE_URL")
        self.email = os.getenv("JIRA_EMAIL")
        self.api_token = os.getenv("JIRA_API_TOKEN")
        self.project_key = os.getenv("JIRA_PROJECT_KEY", "FDBCORE")
        
    @property
    def auth(self):
        return (self.email, self.api_token)
    
    @property
    def headers(self):
        return {"Accept": "application/json", "Content-Type": "application/json"}
    
    def fetch_issues(self, jql: Optional[str] = None, max_results: int = 100) -> list:
        if jql is None:
            jql = f'project = {self.project_key} AND status in ("To Do", "In Progress") ORDER BY priority DESC, created DESC'
        
        url = f"{self.base_url}/rest/api/3/search/jql"
        all_issues = []
        next_page_token = None
        
        while True:
            payload = {
                "jql": jql,
                "maxResults": max_results,
                "fields": ["summary", "status", "priority", "assignee", "created", "updated", "issuetype", "labels", "customfield_11401"]
            }
            if next_page_token:
                payload["nextPageToken"] = next_page_token
            
            response = requests.post(url, headers=self.headers, auth=self.auth, json=payload)
            
            if response.status_code != 200:
                raise Exception(f"JIRA API error: {response.status_code} - {response.text}")
            
            data = response.json()
            issues = data.get("issues", [])
            all_issues.extend(issues)
            
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
        
        return all_issues
    
    def parse_issues(self, issues: list) -> list:
        parsed = []
        for issue in issues:
            fields = issue.get("fields", {})
            assignee = fields.get("assignee")
            priority = fields.get("priority")
            status = fields.get("status")
            
            created_str = fields.get("created", "")
            updated_str = fields.get("updated", "")
            created_date = None
            updated_date = None
            days_open = 0
            days_since_update = 0
            if created_str:
                created_date = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                days_open = (datetime.now(created_date.tzinfo) - created_date).days
            if updated_str:
                updated_date = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                days_since_update = (datetime.now(updated_date.tzinfo) - updated_date).days
            
            priority_name = priority.get("name", "Unknown") if priority else "Unknown"
            
            area_field = fields.get("customfield_11401")
            area = area_field.get("value", "Unassigned") if area_field else "Unassigned"
            
            parsed.append({
                "key": issue.get("key"),
                "summary": fields.get("summary", ""),
                "status": status.get("name", "Unknown") if status else "Unknown",
                "priority": priority_name,
                "assignee": assignee.get("displayName", "Unassigned") if assignee else "Unassigned",
                "assignee_email": assignee.get("emailAddress", "") if assignee else "",
                "created": created_date.strftime("%Y-%m-%d") if created_date else "",
                "days_open": days_open,
                "days_since_update": days_since_update,
                "labels": fields.get("labels", []),
                "area": area,
                "url": f"{self.base_url}/browse/{issue.get('key')}"
            })
        return parsed
    
    def get_fdb_storage_issues(self, custom_jql: Optional[str] = None) -> list:
        if custom_jql:
            jql = custom_jql
        else:
            jql = f'project = {self.project_key} AND status in ("To Do", "In Progress") ORDER BY priority DESC, created DESC'
        issues = self.fetch_issues(jql=jql)
        return self.parse_issues(issues)
