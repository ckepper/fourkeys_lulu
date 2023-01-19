import json
from typing import Dict, List

import gitlab
from gitlab.v4.objects import Project as GitlabProject, ProjectDeployment, ProjectEvent
import config
import os
import csv
import click

from bigquery_helpers import event_exists, insert_into_bigquery
from transformations import transform_deployment, transform_event
from pydantic import ValidationError
from log import root_logger

logger = root_logger.getChild('bulk-migrate-lulu')


def filter_events(project: GitlabProject) -> List[ProjectEvent]:
    events = project.events.list(action="pushed", get_all=True)
    filtered_events = []
    event: ProjectEvent
    for event in events:
        if (
            config.MIN_DATE <= event.created_at <= config.MAX_DATE
            and event.push_data["action"] == "pushed"
        ):
            filtered_events.append(event)
    return filtered_events


def filter_deployments(project: GitlabProject) -> List[ProjectDeployment]:
    deployments = project.deployments.list(all=True)
    filtered_deployments = []
    deployment: ProjectDeployment
    for deployment in deployments:
        time_created = (
            deployment.deployable.get("finished_at")
            or deployment.deployable.get("started_at")
            or deployment.deployable.get("created_at")
        )
        if config.MIN_DATE <= time_created <= config.MAX_DATE:
            filtered_deployments.append(deployment)
    return filtered_deployments


def get_project_ids() -> List[Dict]:
    results = []
    with open(os.path.join(config.ROOT_DIR, "repos.csv")) as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results


@click.command()
@click.option("--project-id", help="GitLab project ID", type=int, required=False)
@click.option("--all", help="Import all projects", is_flag=True, required=False)
def main(project_id: int, all: bool):
    if project_id:
        import_project(project_id)
    if all:
        import_all_projects()


def import_all_projects():
    project_ids = [r["project_id"] for r in get_project_ids()]
    for project_id in project_ids:
        if project_id == 1657:
            continue
        import_project(project_id)


def import_project(project_id):
    gl = gitlab.Gitlab.from_config("lulu")
    p: GitlabProject = gl.projects.get(project_id)
    logger.info(f"Processing project {p.id} ({p.path_with_namespace})")
    # processing events
    events = filter_events(p)
    logger.info(f"Found {len(events)} events")
    i = 0
    batch = []
    for e in events:
        if i % 10 == 0:
            logger.info(f"Processed {i} events")
        if i % config.BATCH_SIZE == 0:
            insert_into_bigquery(batch)
            batch = []
        raw_event = transform_event(p, e)
        batch.append(raw_event)
        i += 1

    if batch:
        insert_into_bigquery(batch)

    # processing deployments
    deployments = filter_deployments(p)
    logger.info(f"Found {len(deployments)} deployments")
    i = 0
    d: ProjectDeployment
    for d in deployments:
        if i % 10 == 0:
            logger.info(f"Processed {i} deployments")
        if d.attributes["environment"]["name"] != "upp-prod":
            continue
        try:
            event = transform_deployment(d)
        except ValidationError as e:
            logger.exception(e)
            print(f"failed to process deployment {d.get_id()}")
            i += 1
            continue
        insert_into_bigquery(event)
        i += 1


if __name__ == "__main__":
    main()
