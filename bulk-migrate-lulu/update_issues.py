import csv
import os

import config
from bigquery_helpers import insert_into_bigquery
from log import root_logger
from models import Incident, Label
from transformations import transform_incident

logger = root_logger.getChild("bulk-migrate-lulu")


def main():
    with open(os.path.join(config.ROOT_DIR, "firelane_issues.csv")) as f:
        reader = csv.DictReader(f)
        incidents = []
        for row in reader:
            raw_incident = Incident(
                id=row["id"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                closed_at=row["closed_at"],
                labels=[Label(title="Incident")],
                description=row["title"] + " " + row["description"],
            )
            incident = transform_incident(raw_incident)
            incidents.append(incident)
        insert_into_bigquery(incidents)
        logger.info(f"Inserted {len(incidents)} incidents into BigQuery")


if __name__ == "__main__":
    main()
