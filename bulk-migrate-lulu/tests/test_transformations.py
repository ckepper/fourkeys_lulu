import json
import os
import gitlab
import pytest
from gitlab.v4.objects import ProjectEvent, Project as GitlabProject
from transformations import get_commits, transform_event, get_project
from models import EventsRaw, Project
import config
import csv


@pytest.mark.skip("integration test")
def test_create_push_metadata():
    # load push_event.json
    with open("tests/data/push_event.json") as f:
        event = json.load(f)
    project_id = 1657
    gl = gitlab.Gitlab.from_config("lulu")
    p: GitlabProject = gl.projects.get(project_id)
    event_dict = {
        "id": 197479,
        "project_id": 1657,
        "action_name": "pushed to",
        "target_id": None,
        "target_iid": None,
        "target_type": None,
        "author_id": 99,
        "target_title": None,
        "created_at": "2023-01-16T08:16:31.473Z",
        "author": {
            "id": 99,
            "username": "sfigiel",
            "name": "Szymon Figiel",
            "state": "active",
            "avatar_url": "https://secure.gravatar.com/avatar/0f250efdd60af734283f00cefa6bc809?s=80&d=identicon",
            "web_url": "https://gitlab.it.lulu.com/sfigiel",
        },
        "push_data": {
            "commit_count": 3,
            "action": "pushed",
            "ref_type": "branch",
            "commit_from": "79353ac8b29163779f78446f7ece385b3c7db15d",
            "commit_to": "0f1c9a2de1e9c234b90a157ab437b53135679740",
            "ref": "LU-4367/populate-project-timeline-with-events",
            "commit_title": "LU-4367 populate the recordTimeline query about AuditEvents. Extend...",
            "ref_count": None,
        },
        "author_username": "sfigiel",
    }
    e: ProjectEvent = ProjectEvent(p.manager, event_dict)
    assert isinstance(e, ProjectEvent)
    assert e.author_username == "sfigiel"
    assert e.push_data["commit_count"] == 3
    # get commits
    commits = get_commits(p, e.push_data["commit_to"], [], e.push_data["commit_count"])
    assert len(commits) == 3

    # transform event
    transformed_event = transform_event(p, e)
    assert isinstance(transformed_event, EventsRaw)
    assert transformed_event.event_type == "push"


@pytest.mark.skip("integration test")
def test_list_events():
    project_id = 1657
    gl = gitlab.Gitlab.from_config("lulu")
    p: GitlabProject = gl.projects.get(project_id)
    events1 = p.events.list(action="push", get_all=True)
    assert len(events1) > 0
    events2 = []
    event: ProjectEvent
    min_created_at = '2023-01-31T08:16:31.473Z'
    for event in events1:
        if config.MIN_DATE <= event.created_at <= config.MAX_DATE:
            events2.append(event)
        if event.created_at < min_created_at:
            min_created_at = event.created_at
    assert len(events2) > 0
    assert min_created_at <= '2023-01-01T00:00:00.000Z'
    assert len(events2) <= len(events1)


@pytest.mark.skip("integration test")
def test_csv_reader():
    # use DictReader to extract data from csv
    with open(os.path.join(config.ROOT_DIR, "repos.csv")) as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(row)
            assert row["project_id"].isdigit()
            assert " " not in row["project_name"].split("/")[-1]


def test_get_project():
    project_id = 1657
    gl = gitlab.Gitlab.from_config("lulu")
    p: GitlabProject = gl.projects.get(project_id)

    # first call by passing the project id
    result = get_project(project_id)
    assert isinstance(result, Project)
    assert result.id == 1657

    # second call by passing the project object
    result2 = get_project(p)
    assert isinstance(result2, Project)
    assert result.id == 1657

    # assert that both results are the same
    for k in result.__dict__.keys():
        assert getattr(result, k) == getattr(result2, k)
