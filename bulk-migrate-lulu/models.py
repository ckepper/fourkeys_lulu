from datetime import datetime, timezone
from typing import Optional, Dict, List

from pydantic import BaseModel, validator


def convert_datetime_for_bigquery(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def transform_to_utc_datetime(dt: datetime) -> datetime:
    return dt.astimezone(tz=timezone.utc)


class Project(BaseModel):
    id: int
    name: str
    description: Optional[str]
    web_url: str
    avatar_url: Optional[str]
    git_ssh_url: str
    git_http_url: str
    namespace: str
    visibility_level: int
    path_with_namespace: str
    default_branch: str
    ci_config_path: Optional[str]
    homepage: str
    url: str
    ssh_url: str
    http_url: str


class User(BaseModel):
    id: int
    name: str
    username: str
    avatar_url: str
    email: str


class DeployMetadata(BaseModel):
    object_kind: str
    status: str
    status_changed_at: datetime
    deployment_id: int
    deployable_id: int
    deployable_url: str
    environment: str
    project: Project
    short_sha: str
    user: User
    user_url: str
    commit_url: str
    commit_title: str

    _normalize_datetimes = validator("status_changed_at", allow_reuse=True)(
        transform_to_utc_datetime
    )

    class Config:
        json_encoders = {
            # custom output for BigQuery
            datetime: convert_datetime_for_bigquery
        }


class Author(BaseModel):
    name: str
    email: str


class Commit(BaseModel):
    id: str
    message: str
    title: str
    timestamp: datetime
    url: str
    author: Author
    added: list[str]
    modified: list[str]
    removed: list[str]

    _normalize_datetimes = validator("timestamp", allow_reuse=True)(transform_to_utc_datetime)

    class Config:
        json_encoders = {
            # custom output for BigQuery
            datetime: convert_datetime_for_bigquery
        }


class Repository(BaseModel):
    name: str
    url: str
    description: Optional[str]
    homepage: str
    git_http_url: str
    git_ssh_url: str
    visibility_level: int


class PushMetadata(BaseModel):
    object_kind: str
    event_name: str
    before: str
    after: str
    ref: str
    checkout_sha: str
    message: Optional[str]
    user_id: int
    user_name: str
    user_username: str
    user_email: str
    user_avatar: str
    project_id: int
    project: Project
    commits: list[Commit]
    total_commits_count: int
    push_options: Dict
    repository: Repository


class Label(BaseModel):
    title: str


class Incident(BaseModel):
    created_at: datetime
    updated_at: datetime
    closed_at: datetime
    id: int
    labels: List[Label]
    description: str

    _normalize_datetimes = validator("created_at", "updated_at", "closed_at", allow_reuse=True)(
        transform_to_utc_datetime
    )

    class Config:
        json_encoders = {
            # custom output for BigQuery
            datetime: convert_datetime_for_bigquery
        }


class IncidentMetadata(BaseModel):
    object_kind: str
    object_attributes: Incident


class EventsRaw(BaseModel):
    event_type: str
    id: str
    metadata: str
    time_created: datetime
    signature: str
    msg_id: str
    source: str

    _normalize_datetimes = validator("time_created", allow_reuse=True)(transform_to_utc_datetime)

    class Config:
        json_encoders = {
            # custom output for BigQuery
            datetime: convert_datetime_for_bigquery
        }
