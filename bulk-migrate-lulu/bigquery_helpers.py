import json
from typing import List

from google.cloud import bigquery

from models import EventsRaw


client = bigquery.Client()

def event_exists(client, deployment_id: str, table_ref: str) -> bool:
    sql = f"""
        SELECT signature 
        FROM {table_ref} 
        WHERE TRUE
            AND event_type = 'deployment' 
            AND id = '{deployment_id}'
    """
    query_job = client.query(sql)
    results = query_job.result()
    return results.total_rows > 0


def insert_into_bigquery(events: EventsRaw or List[EventsRaw]):
    if not events:
        return
    # Set up bigquery instance
    project_id = "devopsmetrics-369710"
    dataset_id = "four_keys"
    table_id = "events_raw"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)

    rows = []
    if isinstance(events, EventsRaw):
        count = 1
        # Insert row
        rows.append(events.dict())
    else:
        count = len(events)
        for event in events:
            rows.append(event.dict())

    bq_errors = client.insert_rows(table, rows)

    # If errors, log to Stackdriver
    if bq_errors:
        entry = {
            "severity": "WARNING",
            "msg": "Row not inserted.",
            "errors": bq_errors,
            "rows": rows,
        }
        print(json.dumps(entry))

    print(f"Inserted {count} rows into {table_ref}")
