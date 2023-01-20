import hashlib
import json
import re
import textwrap
from functools import lru_cache
from typing import List

import gitlab
from gitlab.exceptions import GitlabGetError
from gitlab.v4.objects import (
    ProjectDeployment,
    ProjectEvent,
    Project as GitlabProject,
    ProjectCommit,
)

from log import root_logger
from models import (
    User,
    EventsRaw,
    PushMetadata,
    Project,
    Commit,
    Author,
    Repository,
    DeployMetadata,
    Incident,
    IncidentMetadata,
)

logger = root_logger.getChild("transformations")
branch_re = re.compile(r"Merge branch .* into .*\n\n.*\n\n.* merge request .*!(\d+)")


def get_short_name(project: Project) -> str:
    return project.path_with_namespace.split("/")[-1]


@lru_cache
def get_project(project: int or GitlabProject) -> Project:
    if isinstance(project, int):
        project_id = project
        gl = gitlab.Gitlab.from_config("lulu")
        pa = gl.projects.get(project_id).attributes
    else:
        pa = project.attributes
        project_id = pa["id"]
    return Project(
        id=project_id,
        name=pa["name"],
        description=pa["description"],
        web_url=pa["web_url"],
        avatar_url=pa["avatar_url"],
        git_ssh_url=pa["ssh_url_to_repo"],
        git_http_url=pa["http_url_to_repo"],
        namespace=pa["namespace"]["name"],
        visibility_level=10,  # ???
        path_with_namespace=pa["path_with_namespace"],
        default_branch=pa["default_branch"],
        ci_config_path=pa["ci_config_path"],
        homepage=pa["web_url"],
        url=pa["ssh_url_to_repo"],
        ssh_url=pa["ssh_url_to_repo"],
        http_url=pa["http_url_to_repo"],
    )


def transform_deployment(d: ProjectDeployment) -> EventsRaw:
    hashed = hashlib.sha1(bytes(json.dumps(d.to_json()), "utf-8"))
    signature = hashed.hexdigest()
    project_id = d.attributes["deployable"]["pipeline"]["project_id"]
    project = get_project(project_id)

    user = User(
        id=d.attributes["user"]["id"],
        name=d.attributes["user"]["name"],
        username=d.attributes["user"]["username"],
        avatar_url=d.attributes["user"]["avatar_url"],
        email="[REDACTED]",
    )

    time_created = (
        d.attributes["deployable"].get("finished_at")
        or d.attributes["deployable"].get("started_at")
        or d.attributes["deployable"].get("created_at")
    )

    metadata = DeployMetadata(
        object_kind="deployment",
        status=d.attributes["deployable"]["status"],
        status_changed_at=time_created,
        deployment_id=d.get_id(),
        deployable_id=d.attributes["deployable"]["id"],
        deployable_url=d.attributes["deployable"]["web_url"],
        environment=d.attributes["environment"]["slug"],
        project=project,
        short_sha=d.attributes["deployable"]["commit"]["short_id"],
        user=user,
        user_url=d.attributes["user"]["web_url"],
        commit_url=d.attributes["deployable"]["commit"]["web_url"],
        commit_title=d.attributes["deployable"]["commit"]["title"],
    )

    e = EventsRaw(
        event_type="deployment",
        id=d.get_id(),
        metadata=metadata.json(),
        time_created=d.attributes["updated_at"],
        signature=signature,
        msg_id=f"bulk_import_{get_short_name(project)}_{d.get_id()}",
        source=f"gitlab_bulk_import_{get_short_name(project)}",
    )
    return e


def transform_repository(project: GitlabProject) -> Repository:
    """Transforms repo details as shown in events."""
    return Repository(
        name=project.name,
        url=project.ssh_url_to_repo,
        description=project.description,
        homepage=project.web_url,
        git_http_url=project.http_url_to_repo,
        git_ssh_url=project.ssh_url_to_repo,
        visibility_level=10,
    )


def transform_event(project: GitlabProject, e: ProjectEvent) -> EventsRaw:
    project_data = get_project(project.id)
    if e.push_data["commit_count"] > 10:
        logger.info(
            f"Found {e.push_data['commit_count']} commits for {e.push_data['commit_to']}, fetching them all..."
        )
    commits = get_commits(project, e.push_data["commit_to"], [], e.push_data["commit_count"])
    for c in commits:
        if c.id == e.push_data["commit_to"]:
            c.authored_date = e.created_at
    commit_data = [transform_commit(c) for c in commits]
    commit_data.reverse()
    metadata = PushMetadata(
        object_kind="push",
        event_name="push",
        before=e.push_data["commit_from"],
        after=e.push_data["commit_to"],
        ref="refs/heads/" + e.push_data["ref"],
        checkout_sha=e.push_data["commit_to"],
        message=None,
        user_id=e.author_id,
        user_name=e.author["name"],
        user_username=e.author["username"],
        user_email="",
        user_avatar=e.author["avatar_url"],
        project_id=project.id,
        project=project_data,
        commits=commit_data,
        total_commits_count=e.push_data["commit_count"],
        push_options={},
        repository=transform_repository(project),
    )

    hashed = hashlib.sha1(bytes(json.dumps(metadata.json()), "utf-8"))
    signature = hashed.hexdigest()

    e = EventsRaw(
        event_type="push",
        id=e.push_data["commit_to"],
        metadata=metadata.json(),
        time_created=e.created_at,
        signature=signature,
        msg_id=f"bulk_import_{get_short_name(project)}_{e.id}",
        source=f"gitlab_bulk_import_{get_short_name(project)}",
    )
    return e


def get_commits(project: GitlabProject, commit_hash: str, commits: List, count: int) -> List:
    if count == 0:
        return commits
    try:
        commit = project.commits.get(commit_hash)
    except GitlabGetError as e:
        logger.error(f"Error getting commit {commit_hash}: {e}")
        return commits
    commits.append(commit)
    count -= 1
    if len(commit.parent_ids) > 1:
        # analyze if it's a merge commit with a regex
        match = branch_re.match(commit.message)
        if match:
            # if it is, get the merge request and get the commits from there
            mr = project.mergerequests.get(match.group(1))
            for c in mr.commits():
                commits.append(c)
                count -= 1
            logger.info(f"Found {len(mr.commits())} commits in merge request {mr.iid}")
            return commits
        logger.info(
            f"Found {len(commit.parent_ids)} parents for {commit.get_id()}, fetching them all..."
        )
    for sha in commit.parent_ids:
        get_commits(project, sha, commits, count)
    return commits


def transform_commit(c: ProjectCommit) -> Commit:
    added = []
    modified = []
    removed = []

    for change in c.diff(get_all=True):
        if change["new_file"] is True:
            added.append(change["new_path"])
        elif change["deleted_file"] is True:
            removed.append(change["old_path"])
        else:
            modified.append(change["new_path"])

    return Commit(
        id=c.id,
        message=c.message,
        title=textwrap.shorten(c.title, width=80, placeholder="..."),
        timestamp=c.authored_date,
        url=c.web_url,
        author=Author(name=c.author_name, email=c.author_email),
        added=added,
        modified=modified,
        removed=removed,
    )


def transform_incident(i: Incident) -> EventsRaw:
    metadata = IncidentMetadata(object_kind="incident", object_attributes=i)
    hashed = hashlib.sha1(bytes(json.dumps(metadata.json()), "utf-8"))
    signature = hashed.hexdigest()
    e = EventsRaw(
        event_type="issue",
        id=f"issue_{i.id}",
        metadata=metadata.json(),
        time_created=i.created_at,
        signature=signature,
        msg_id=f"bulk_import_incident_{i.id}",
        source=f"gitlab_bulk_import_incidents",
    )
    return e
