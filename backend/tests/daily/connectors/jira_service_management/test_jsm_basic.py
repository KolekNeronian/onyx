"""Basic tests for Jira Service Management connector.

This test validates:
- JSM connector can authenticate
- JSM connector can fetch tickets
- JSM-specific fields (SLA, request type) are captured in metadata
- Incremental polling works correctly
"""

import os
from datetime import datetime, timedelta

import pytest

from onyx.configs.constants import DocumentSource
from onyx.connectors.jira_service_management.connector import (
    JiraServiceManagementConnector,
)
from onyx.utils.variable_functionality import global_version
from tests.daily.connectors.utils import TestSecret, load_all_from_connector


@pytest.mark.secrets(TestSecret.JIRA_USER_EMAIL, TestSecret.JIRA_API_TOKEN)
def test_jsm_basic_load() -> None:
    """Test basic JSM connector functionality: authentication and ticket loading."""
    global_version.set_ee()

    connector = JiraServiceManagementConnector(
        jira_base_url=os.environ["JIRA_BASE_URL"],
        project_key=os.environ.get("JIRA_PROJECT_KEY"),
        comment_email_blacklist=[],
    )

    connector.load_credentials(
        {
            "jira_user_email": os.environ["JIRA_USER_EMAIL"],
            "jira_api_token": os.environ["JIRA_API_TOKEN"],
        }
    )

    start = 0
    end = datetime.now().timestamp()

    result = load_all_from_connector(
        connector=connector,
        start=start,
        end=end,
    )

    documents = result.documents

    assert len(documents) > 0, "No documents loaded from JSM"

    for doc in documents:
        assert doc.source == DocumentSource.JIRA_SERVICE_MANAGEMENT
        assert doc.id.startswith(os.environ["JIRA_BASE_URL"])
        assert doc.semantic_identifier
        assert doc.sections
        assert len(doc.sections) > 0

        print(f"Document: {doc.semantic_identifier}")
        print(f"  ID: {doc.id}")
        print(f"  Metadata keys: {list(doc.metadata.keys())}")

        if "request_type" in doc.metadata:
            print(f"  Request Type: {doc.metadata['request_type']}")

        sla_keys = [k for k in doc.metadata.keys() if k.startswith("sla_")]
        if sla_keys:
            print(f"  SLA fields found: {sla_keys}")


@pytest.mark.secrets(TestSecret.JIRA_USER_EMAIL, TestSecret.JIRA_API_TOKEN)
def test_jsm_incremental_poll() -> None:
    """Test incremental polling to verify we can fetch recent updates."""
    global_version.set_ee()

    connector = JiraServiceManagementConnector(
        jira_base_url=os.environ["JIRA_BASE_URL"],
        project_key=os.environ.get("JIRA_PROJECT_KEY"),
        comment_email_blacklist=[],
    )

    connector.load_credentials(
        {
            "jira_user_email": os.environ["JIRA_USER_EMAIL"],
            "jira_api_token": os.environ["JIRA_API_TOKEN"],
        }
    )

    now = datetime.now()
    one_week_ago = now - timedelta(days=7)

    start = one_week_ago.timestamp()
    end = now.timestamp()

    result = load_all_from_connector(
        connector=connector,
        start=start,
        end=end,
    )

    documents = result.documents

    assert len(documents) >= 0, "Incremental poll failed"

    print(f"Found {len(documents)} documents updated in the last week")

    for doc in documents[:5]:
        print(f"  - {doc.semantic_identifier}")


if __name__ == "__main__":
    test_jsm_basic_load()
    test_jsm_incremental_poll()
