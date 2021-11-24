from typing import Optional
from datetime import datetime, timezone

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()
DATE_FORMAT = "%Y-%m-%d"


def get_time_range(
    dataset: str,
    table: str,
    start: Optional[str],
    end: Optional[str],
) -> tuple[datetime, datetime]:
    if start and end:
        _start, _end = [
            datetime.strptime(i, DATE_FORMAT).replace(tzinfo=timezone.utc)
            for i in [start, end]
        ]
    else:
        results = BQ_CLIENT.query(
            f"SELECT MAX(created) AS max_incre FROM {dataset}.{table}"
        ).result()
        _start = [dict(row.items()) for row in results][0]["max_incre"]
        _end = datetime.utcnow()
    return _start, _end


def load(dataset: str, table: str, schema: list[dict], rows: list[dict]) -> int:
    output_rows = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        )
        .result()
        .output_rows
    )
    update(dataset, table)
    return output_rows


def update(dataset: str, table: str) -> None:
    BQ_CLIENT.query(
        f"""
        CREATE OR REPLACE TABLE {dataset}.{table}
        AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *, ROW_NUMBER() OVER
            (PARTITION BY id) AS row_num
            FROM {dataset}.{table}
        )
        WHERE row_num = 1
        """
    )
