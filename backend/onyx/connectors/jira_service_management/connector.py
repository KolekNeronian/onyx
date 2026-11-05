import copy
from collections.abc import Generator, Iterator
from datetime import datetime
from typing import Any

from typing_extensions import override

from onyx.configs.app_configs import (
    INDEX_BATCH_SIZE,
    JIRA_CONNECTOR_LABELS_TO_SKIP,
    JIRA_CONNECTOR_MAX_TICKET_SIZE,
    JIRA_SLIM_PAGE_SIZE,
)
from onyx.configs.constants import DocumentSource
from onyx.connectors.credentials_provider import OnyxStaticCredentialsProvider
from onyx.connectors.cross_connector_utils.miscellaneous_utils import (
    time_str_to_utc,
)
from onyx.connectors.exceptions import ConnectorMissingCredentialError
from onyx.connectors.interfaces import (
    CheckpointedConnectorWithPermSync,
    CheckpointOutput,
    GenerateSlimDocumentOutput,
    Resolver,
    SecondsSinceUnixEpoch,
    SlimConnector,
    SlimConnectorWithPermSync,
)
from onyx.connectors.jira.access import get_project_permissions
from onyx.connectors.jira.connector import (
    _FIELD_ASSIGNEE,
    _FIELD_ASSIGNEE_EMAIL,
    _FIELD_CREATED,
    _FIELD_DUEDATE,
    _FIELD_ISSUETYPE,
    _FIELD_KEY,
    _FIELD_LABELS,
    _FIELD_PARENT,
    _FIELD_PRIORITY,
    _FIELD_PROJECT,
    _FIELD_PROJECT_NAME,
    _FIELD_REPORTER,
    _FIELD_REPORTER_EMAIL,
    _FIELD_RESOLUTION,
    _FIELD_RESOLUTION_DATE,
    _FIELD_RESOLUTION_DATE_KEY,
    _FIELD_STATUS,
    _FIELD_UPDATED,
    JiraIssue,
    _issue_key,
    _parse_issue_key,
    _perform_jql_search,
    build_jql_query,
    make_checkpoint_callback,
    scope_jql,
)
from onyx.connectors.jira.source_operations import JiraSourceOperations
from onyx.connectors.jira.utils import (
    best_effort_basic_expert_info,
    build_jira_url,
    get_comment_strs,
    get_issue_field,
    get_named_field,
    rich_text,
)
from onyx.connectors.jira_service_management.models import JiraIssueIdPage  # noqa: F401
from onyx.connectors.jira_service_management.utils import (
    extract_jsm_approvals,
    extract_jsm_request_type,
    extract_jsm_sla_info,
)
from onyx.connectors.models import (
    ConnectorCheckpoint,
    ConnectorFailure,
    Document,
    DocumentFailure,
    HierarchyNode,
    SlimDocument,
    TextSection,
)
from onyx.db.enums import HierarchyNodeType
from onyx.indexing.indexing_heartbeat import IndexingHeartbeatInterface
from onyx.utils.logger import setup_logger

logger = setup_logger()

_JIRA_FULL_PAGE_SIZE = 50


def process_jsm_issue(
    jira_base_url: str,
    issue: JiraIssue,
    comment_email_blacklist: tuple[str, ...] = (),
    labels_to_skip: set[str] | None = None,
    parent_hierarchy_raw_node_id: str | None = None,
) -> Document | None:
    """Process a JSM issue into a Document, enriching with JSM-specific fields."""
    issue_key: str = _issue_key(issue)
    issue_labels: list[str] = get_issue_field(issue, _FIELD_LABELS) or []
    if labels_to_skip:
        if any(label in issue_labels for label in labels_to_skip):
            logger.info(
                "Skipping %s because it has a label to skip. Found labels: %s. Labels to skip: %s.",
                issue_key,
                issue_labels,
                labels_to_skip,
            )
            return None

    description: str = rich_text(get_issue_field(issue, "description"))

    comments: list[str] = get_comment_strs(
        issue=issue,
        comment_email_blacklist=comment_email_blacklist,
    )
    ticket_content = f"{description}\n" + "\n".join(
        [f"Comment: {comment}" for comment in comments if comment]
    )

    if len(ticket_content.encode("utf-8")) > JIRA_CONNECTOR_MAX_TICKET_SIZE:
        logger.info(
            "Skipping %s because it exceeds the maximum size of %s bytes.",
            issue_key,
            JIRA_CONNECTOR_MAX_TICKET_SIZE,
        )
        return None

    page_url = build_jira_url(jira_base_url, issue_key)

    metadata_dict: dict[str, str | list[str]] = {}
    people = set()

    creator: Any = get_issue_field(issue, _FIELD_REPORTER)
    if creator is not None and (
        basic_expert_info := best_effort_basic_expert_info(creator)
    ):
        people.add(basic_expert_info)
        metadata_dict[_FIELD_REPORTER] = basic_expert_info.get_semantic_name()
        if email := basic_expert_info.get_email():
            metadata_dict[_FIELD_REPORTER_EMAIL] = email

    assignee: Any = get_issue_field(issue, _FIELD_ASSIGNEE)
    if assignee is not None and (
        basic_expert_info := best_effort_basic_expert_info(assignee)
    ):
        people.add(basic_expert_info)
        metadata_dict[_FIELD_ASSIGNEE] = basic_expert_info.get_semantic_name()
        if email := basic_expert_info.get_email():
            metadata_dict[_FIELD_ASSIGNEE_EMAIL] = email

    metadata_dict[_FIELD_KEY] = issue_key
    if priority := get_named_field(issue, _FIELD_PRIORITY):
        metadata_dict[_FIELD_PRIORITY] = priority
    if status := get_named_field(issue, _FIELD_STATUS):
        metadata_dict[_FIELD_STATUS] = status
    if resolution := get_named_field(issue, _FIELD_RESOLUTION):
        metadata_dict[_FIELD_RESOLUTION] = resolution
    if issue_labels:
        metadata_dict[_FIELD_LABELS] = issue_labels
    if created := get_issue_field(issue, _FIELD_CREATED):
        metadata_dict[_FIELD_CREATED] = created
    if updated := get_issue_field(issue, _FIELD_UPDATED):
        metadata_dict[_FIELD_UPDATED] = updated
    if duedate := get_issue_field(issue, _FIELD_DUEDATE):
        metadata_dict[_FIELD_DUEDATE] = duedate
    if issuetype := get_named_field(issue, _FIELD_ISSUETYPE):
        metadata_dict[_FIELD_ISSUETYPE] = issuetype
    if resolutiondate := get_issue_field(issue, _FIELD_RESOLUTION_DATE):
        metadata_dict[_FIELD_RESOLUTION_DATE_KEY] = resolutiondate

    parent: Any = get_issue_field(issue, _FIELD_PARENT)
    if parent is not None:
        metadata_dict[_FIELD_PARENT] = parent[_FIELD_KEY]

    project: Any = get_issue_field(issue, _FIELD_PROJECT)
    if project is not None:
        metadata_dict[_FIELD_PROJECT_NAME] = project["name"]
        metadata_dict[_FIELD_PROJECT] = project[_FIELD_KEY]
    else:
        logger.error("Project should exist but does not for %s", issue_key)

    request_type = extract_jsm_request_type(issue)
    if request_type:
        metadata_dict["request_type"] = request_type

    sla_info = extract_jsm_sla_info(issue)
    if sla_info:
        for key, value in sla_info.items():
            metadata_dict[f"sla_{key}"] = value

    approval_status = extract_jsm_approvals(issue)
    if approval_status:
        metadata_dict["approval_status"] = approval_status

    summary: Any = get_issue_field(issue, "summary")
    return Document(
        id=page_url,
        sections=[TextSection(link=page_url, text=ticket_content)],
        source=DocumentSource.JIRA_SERVICE_MANAGEMENT,
        semantic_identifier=f"{issue_key}: {summary}",
        title=f"{issue_key} {summary}",
        doc_updated_at=time_str_to_utc(get_issue_field(issue, _FIELD_UPDATED)),
        doc_created_at=time_str_to_utc(get_issue_field(issue, _FIELD_CREATED)),
        primary_owners=list(people) or None,
        metadata=metadata_dict,
        parent_hierarchy_raw_node_id=parent_hierarchy_raw_node_id,
    )


class JiraServiceManagementConnectorCheckpoint(ConnectorCheckpoint):
    all_issue_ids: list[list[str]] = []
    ids_done: bool = False
    cursor: str | None = None
    offset: int | None = None
    seen_hierarchy_node_ids: list[str] = []


class JiraServiceManagementConnector(
    CheckpointedConnectorWithPermSync[JiraServiceManagementConnectorCheckpoint],
    SlimConnector,
    SlimConnectorWithPermSync,
    Resolver,
):
    slim_listing_honors_indexing_start = True

    def __init__(
        self,
        jira_base_url: str,
        project_key: str | None = None,
        comment_email_blacklist: list[str] | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
        labels_to_skip: list[str] = JIRA_CONNECTOR_LABELS_TO_SKIP,
        jql_query: str | None = None,
        scoped_token: bool = False,
    ) -> None:
        self.batch_size = batch_size
        self.jira_base = jira_base_url.rstrip("/")
        self.jira_project = (project_key or "").strip() or None
        self._comment_email_blacklist = comment_email_blacklist or []
        self.labels_to_skip = set(labels_to_skip)
        self.jql_query = (jql_query or "").strip() or None
        self.scoped_token = scoped_token
        self._source_operations: JiraSourceOperations | None = None
        self._project_permissions_cache: dict[str, Any] = {}

    @property
    def comment_email_blacklist(self) -> tuple:
        return tuple(email.strip() for email in self._comment_email_blacklist)

    @property
    def source_operations(self) -> JiraSourceOperations:
        if self._source_operations is None:
            raise ConnectorMissingCredentialError("Jira Service Management")
        return self._source_operations

    def _get_project_permissions(
        self, project_key: str, add_prefix: bool = False
    ) -> Any:
        cache_key = f"{project_key}:{'prefixed' if add_prefix else 'unprefixed'}"
        if cache_key not in self._project_permissions_cache:
            self._project_permissions_cache[cache_key] = get_project_permissions(
                source_operations=self.source_operations,
                jira_project=project_key,
                add_prefix=add_prefix,
            )
        return self._project_permissions_cache[cache_key]

    def _is_epic(self, issue: JiraIssue) -> bool:
        issuetype: str | None = get_named_field(issue, _FIELD_ISSUETYPE)
        return issuetype is not None and issuetype.lower() == "epic"

    def _yield_project_hierarchy_node(
        self,
        project_key: str,
        project_name: str | None,
        seen_hierarchy_node_ids: set[str],
    ) -> Generator[HierarchyNode, None, None]:
        if project_key in seen_hierarchy_node_ids:
            return

        seen_hierarchy_node_ids.add(project_key)

        yield HierarchyNode(
            raw_node_id=project_key,
            raw_parent_id=None,
            display_name=project_name or project_key,
            link=f"{self.jira_base}/projects/{project_key}",
            node_type=HierarchyNodeType.PROJECT,
        )

    def _yield_epic_hierarchy_node(
        self,
        issue: JiraIssue,
        project_key: str,
        seen_hierarchy_node_ids: set[str],
    ) -> Generator[HierarchyNode, None, None]:
        issue_key = _issue_key(issue)
        if issue_key in seen_hierarchy_node_ids:
            return

        seen_hierarchy_node_ids.add(issue_key)

        yield HierarchyNode(
            raw_node_id=issue_key,
            raw_parent_id=project_key,
            display_name=f"{issue_key}: {get_issue_field(issue, 'summary')}",
            link=build_jira_url(self.jira_base, issue_key),
            node_type=HierarchyNodeType.FOLDER,
        )

    def _yield_parent_hierarchy_node_if_epic(
        self,
        parent: JiraIssue,
        project_key: str,
        seen_hierarchy_node_ids: set[str],
    ) -> Generator[HierarchyNode, None, None]:
        parent_key = _issue_key(parent)
        if parent_key in seen_hierarchy_node_ids:
            return

        if not self._is_epic(parent):
            return

        seen_hierarchy_node_ids.add(parent_key)

        parent_summary: Any = get_issue_field(parent, "summary")
        display_name = (
            f"{parent_key}: {parent_summary}" if parent_summary else parent_key
        )

        yield HierarchyNode(
            raw_node_id=parent_key,
            raw_parent_id=project_key,
            display_name=display_name,
            link=build_jira_url(self.jira_base, parent_key),
            node_type=HierarchyNodeType.FOLDER,
        )

    def _get_parent_hierarchy_raw_node_id(
        self, issue: JiraIssue, project_key: str
    ) -> str:
        parent: Any = get_issue_field(issue, _FIELD_PARENT)
        if parent is None:
            return project_key

        if self._is_epic(parent):
            return _issue_key(parent)

        return project_key

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        self._source_operations = JiraSourceOperations(
            credentials_provider=OnyxStaticCredentialsProvider(
                None, DocumentSource.JIRA_SERVICE_MANAGEMENT.value, credentials
            ),
            connector_specific_config={
                "jira_base_url": self.jira_base,
                "scoped_token": self.scoped_token,
            },
        )
        return None

    def _get_jql_query(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> str:
        return build_jql_query(
            project_key=self.jira_project,
            jql_query=self.jql_query,
            start=start,
            end=end,
        )

    def load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: JiraServiceManagementConnectorCheckpoint,
    ) -> CheckpointOutput[JiraServiceManagementConnectorCheckpoint]:
        return self._load_from_checkpoint(
            self._get_jql_query(start, end),
            checkpoint,
            include_permissions=False,
        )

    def load_from_checkpoint_with_perm_sync(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: JiraServiceManagementConnectorCheckpoint,
    ) -> CheckpointOutput[JiraServiceManagementConnectorCheckpoint]:
        return self._load_from_checkpoint(
            self._get_jql_query(start, end),
            checkpoint,
            include_permissions=True,
        )

    def _load_from_checkpoint(
        self,
        jql: str,
        checkpoint: JiraServiceManagementConnectorCheckpoint,
        include_permissions: bool,
    ) -> CheckpointOutput[JiraServiceManagementConnectorCheckpoint]:
        starting_offset = checkpoint.offset or 0
        current_offset = starting_offset
        new_checkpoint = copy.deepcopy(checkpoint)

        seen_hierarchy_node_ids = set(new_checkpoint.seen_hierarchy_node_ids)

        checkpoint_callback = make_checkpoint_callback(new_checkpoint)

        for issue in _perform_jql_search(
            source_operations=self.source_operations,
            jql=jql,
            start=current_offset,
            max_results=_JIRA_FULL_PAGE_SIZE,
            all_issue_ids=new_checkpoint.all_issue_ids,
            checkpoint_callback=checkpoint_callback,
            nextPageToken=new_checkpoint.cursor,
            ids_done=new_checkpoint.ids_done,
        ):
            issue_key = _issue_key(issue)
            try:
                yield from self._issue_to_items(
                    issue, seen_hierarchy_node_ids, include_permissions
                )
            except Exception as e:
                yield ConnectorFailure(
                    failed_document=DocumentFailure(
                        document_id=build_jira_url(self.jira_base, issue_key),
                        document_link=build_jira_url(self.jira_base, issue_key),
                    ),
                    failure_message=f"Failed to process JSM issue: {str(e)}",
                    exception=e,
                )

            current_offset += 1

        new_checkpoint.seen_hierarchy_node_ids = list(seen_hierarchy_node_ids)

        from onyx.connectors.jira.connector import JiraConnector

        jira_connector = JiraConnector(
            jira_base_url=self.jira_base,
            project_key=self.jira_project,
            jql_query=self.jql_query,
        )
        jira_connector._source_operations = self._source_operations
        jira_connector.update_checkpoint_for_next_run(
            new_checkpoint, current_offset, starting_offset, _JIRA_FULL_PAGE_SIZE
        )

        return new_checkpoint

    def _issue_to_items(
        self,
        issue: JiraIssue,
        seen_hierarchy_node_ids: set[str],
        include_permissions: bool,
    ) -> Generator[Document | HierarchyNode, None, None]:
        project = get_issue_field(issue, _FIELD_PROJECT)
        project_key = project[_FIELD_KEY] if project else None
        project_name = project.get("name") if project else None

        if project_key:
            yield from self._yield_project_hierarchy_node(
                project_key, project_name, seen_hierarchy_node_ids
            )

            parent = get_issue_field(issue, _FIELD_PARENT)
            if parent:
                yield from self._yield_parent_hierarchy_node_if_epic(
                    parent, project_key, seen_hierarchy_node_ids
                )

            if self._is_epic(issue):
                yield from self._yield_epic_hierarchy_node(
                    issue, project_key, seen_hierarchy_node_ids
                )

        parent_hierarchy_raw_node_id = (
            self._get_parent_hierarchy_raw_node_id(issue, project_key)
            if project_key
            else None
        )

        if document := process_jsm_issue(
            jira_base_url=self.jira_base,
            issue=issue,
            comment_email_blacklist=self.comment_email_blacklist,
            labels_to_skip=self.labels_to_skip,
            parent_hierarchy_raw_node_id=parent_hierarchy_raw_node_id,
        ):
            if include_permissions:
                document.external_access = self._get_project_permissions(
                    project_key,  # type: ignore[arg-type]
                    add_prefix=True,
                )
            yield document

    def retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        yield from self._retrieve_all_slim_docs(
            start=start, end=end, callback=callback, include_permissions=False
        )

    def retrieve_all_slim_docs_perm_sync(
        self,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        yield from self._retrieve_all_slim_docs(
            start=start, end=end, callback=callback, include_permissions=True
        )

    def _retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
        callback: IndexingHeartbeatInterface | None = None,
        *,
        include_permissions: bool,
    ) -> GenerateSlimDocumentOutput:
        from datetime import timedelta

        one_day = timedelta(hours=24).total_seconds()

        start = start or 0
        end = end or datetime.now().timestamp() + one_day

        jql = self._get_jql_query(start, end)
        checkpoint = self.build_dummy_checkpoint()
        checkpoint_callback = make_checkpoint_callback(checkpoint)
        prev_offset = 0
        current_offset = 0
        slim_doc_batch: list[SlimDocument | HierarchyNode] = []

        seen_hierarchy_node_ids: set[str] = set()

        while checkpoint.has_more:
            for issue in _perform_jql_search(
                source_operations=self.source_operations,
                jql=jql,
                start=current_offset,
                max_results=JIRA_SLIM_PAGE_SIZE,
                all_issue_ids=checkpoint.all_issue_ids,
                checkpoint_callback=checkpoint_callback,
                nextPageToken=checkpoint.cursor,
                ids_done=checkpoint.ids_done,
            ):
                project = get_issue_field(issue, _FIELD_PROJECT)
                project_key = project[_FIELD_KEY] if project else None
                project_name = project.get("name") if project else None

                if not project_key:
                    continue

                slim_doc_batch.extend(
                    self._yield_project_hierarchy_node(
                        project_key, project_name, seen_hierarchy_node_ids
                    )
                )

                parent = get_issue_field(issue, _FIELD_PARENT)
                if parent:
                    slim_doc_batch.extend(
                        self._yield_parent_hierarchy_node_if_epic(
                            parent, project_key, seen_hierarchy_node_ids
                        )
                    )

                if self._is_epic(issue):
                    slim_doc_batch.extend(
                        self._yield_epic_hierarchy_node(
                            issue, project_key, seen_hierarchy_node_ids
                        )
                    )

                doc_id = build_jira_url(self.jira_base, _issue_key(issue))

                created = get_issue_field(issue, _FIELD_CREATED)

                slim_doc_batch.append(
                    SlimDocument(
                        id=doc_id,
                        external_access=(
                            self._get_project_permissions(project_key, add_prefix=False)
                            if include_permissions
                            else None
                        ),
                        parent_hierarchy_raw_node_id=(
                            self._get_parent_hierarchy_raw_node_id(issue, project_key)
                            if project_key
                            else None
                        ),
                        doc_created_at=time_str_to_utc(created) if created else None,
                    )
                )
                current_offset += 1
                if len(slim_doc_batch) >= JIRA_SLIM_PAGE_SIZE:
                    yield slim_doc_batch
                    slim_doc_batch = []

            from onyx.connectors.jira.connector import JiraConnector

            jira_connector = JiraConnector(
                jira_base_url=self.jira_base,
                project_key=self.jira_project,
                jql_query=self.jql_query,
            )
            jira_connector._source_operations = self.source_operations
            jira_connector.update_checkpoint_for_next_run(
                checkpoint, current_offset, prev_offset, JIRA_SLIM_PAGE_SIZE
            )
            prev_offset = current_offset

        if slim_doc_batch:
            yield slim_doc_batch

    def validate_connector_settings(self) -> None:
        from onyx.connectors.jira.connector import JiraConnector

        jira_connector = JiraConnector(
            jira_base_url=self.jira_base,
            project_key=self.jira_project,
            comment_email_blacklist=list(self._comment_email_blacklist),
            batch_size=self.batch_size,
            labels_to_skip=list(self.labels_to_skip),
            jql_query=self.jql_query,
            scoped_token=self.scoped_token,
        )
        jira_connector._source_operations = self._source_operations
        jira_connector.validate_connector_settings()

    def reindex(
        self,
        errors: list[ConnectorFailure],
        include_permissions: bool = False,
    ) -> Generator[Document | ConnectorFailure | HierarchyNode, None, None]:
        key_by_doc_id: dict[str, str] = {}
        for failure in errors:
            if failure.failed_document is None:
                continue
            doc_id = failure.failed_document.document_id
            if doc_id in key_by_doc_id:
                continue
            issue_key = _parse_issue_key(self.jira_base, doc_id)
            if issue_key is None:
                yield ConnectorFailure(
                    failed_document=DocumentFailure(
                        document_id=doc_id, document_link=doc_id
                    ),
                    failure_message=(
                        f"'{doc_id}' is not a JSM issue URL of the form "
                        f"{self.jira_base}/browse/<ISSUE-KEY>"
                    ),
                )
                continue
            key_by_doc_id[doc_id] = issue_key

        if not key_by_doc_id:
            return

        from onyx.connectors.jira.connector import JiraConnector

        jira_connector = JiraConnector(
            jira_base_url=self.jira_base,
            project_key=self.jira_project,
            jql_query=self.jql_query,
        )
        jira_connector._source_operations = self._source_operations

        seen_hierarchy_node_ids: set[str] = set()
        reindexed_doc_ids: set[str] = set()

        for item in jira_connector.reindex(errors, include_permissions):
            if isinstance(item, Document):
                item.source = DocumentSource.JIRA_SERVICE_MANAGEMENT
                reindexed_doc_ids.add(item.id)
            elif isinstance(item, HierarchyNode):
                if item.raw_node_id not in seen_hierarchy_node_ids:
                    seen_hierarchy_node_ids.add(item.raw_node_id)
            yield item

    @override
    def validate_checkpoint_json(
        self, checkpoint_json: str
    ) -> JiraServiceManagementConnectorCheckpoint:
        return JiraServiceManagementConnectorCheckpoint.model_validate_json(
            checkpoint_json
        )

    @override
    def build_dummy_checkpoint(self) -> JiraServiceManagementConnectorCheckpoint:
        return JiraServiceManagementConnectorCheckpoint(
            has_more=True,
        )


if __name__ == "__main__":
    import os

    from onyx.utils.variable_functionality import global_version
    from tests.daily.connectors.utils import load_all_from_connector

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

    for slim_doc in connector.retrieve_all_slim_docs_perm_sync(
        start=start,
        end=end,
    ):
        print(slim_doc)

    for doc in load_all_from_connector(
        connector=connector,
        start=start,
        end=end,
    ).documents:
        print(doc)
