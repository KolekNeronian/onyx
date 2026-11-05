from typing import Any

from onyx.connectors.jira.utils import (
    best_effort_basic_expert_info,
    build_jira_url,
    extract_jira_project,
    extract_text_from_adf,
    get_comment_strs,
    get_issue_field,
    get_named_field,
    rich_text,
)
from onyx.utils.logger import setup_logger

logger = setup_logger()

__all__ = [
    "best_effort_basic_expert_info",
    "build_jira_url",
    "extract_jira_project",
    "extract_text_from_adf",
    "get_comment_strs",
    "get_issue_field",
    "get_named_field",
    "rich_text",
    "extract_jsm_request_type",
    "extract_jsm_sla_info",
    "extract_jsm_approvals",
]


def extract_jsm_request_type(issue: dict[str, Any]) -> str | None:
    """Extract JSM request type from an issue."""
    request_type = get_issue_field(issue, "customfield_10010")
    if isinstance(request_type, dict):
        return request_type.get("requestType", {}).get("name")
    return None


def extract_jsm_sla_info(issue: dict[str, Any]) -> dict[str, Any]:
    """Extract JSM SLA information from an issue.
    
    Returns a dictionary with SLA fields like time_to_resolution,
    time_to_first_response, and breached status if available.
    """
    sla_info: dict[str, Any] = {}
    
    time_to_resolution = get_issue_field(issue, "customfield_10011")
    if time_to_resolution is not None:
        sla_info["time_to_resolution"] = str(time_to_resolution)
    
    time_to_first_response = get_issue_field(issue, "customfield_10012")
    if time_to_first_response is not None:
        sla_info["time_to_first_response"] = str(time_to_first_response)
    
    sla_breached = get_issue_field(issue, "customfield_10013")
    if sla_breached is not None:
        sla_info["sla_breached"] = str(sla_breached)
    
    return sla_info


def extract_jsm_approvals(issue: dict[str, Any]) -> str | None:
    """Extract JSM approval status from an issue."""
    approvals = get_issue_field(issue, "customfield_10009")
    if isinstance(approvals, dict):
        return approvals.get("approvalDecision")
    elif isinstance(approvals, str):
        return approvals
    return None
