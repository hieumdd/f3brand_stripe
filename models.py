import os
import json
from datetime import datetime

import stripe
from google.cloud import bigquery


stripe.api_key = os.getenv("API_KEY")

DATASET = "stripe"
BQ_CLIENT = bigquery.Client()


class BalanceTransactions:
    table = "BalanceTransactions"

    def __init__(self, start, end):
        self.start, self.end = self._get_time_range(start, end)
        self.keys, self.schema = self._get_config()

    def _get_time_range(self, start, end):
        if start and end:
            start, end = [
                round(datetime.strptime(i, "%Y-%m-%d").timestamp())
                for i in [start, end]
            ]
        else:
            query = f"""
            SELECT UNIX_SECONDS(MAX({self.keys.get('incre_key')})) AS incre
            FROM {DATASET}.{self.table}
            """
            try:
                results = BQ_CLIENT.query(query).result()
                row = [row for row in results][0]
                start = row["incre"]
            except:
                start = round(datetime(2021, 1, 1).timestamp())
            end = round(datetime.now().timestamp())

        return start, end

    def _get_config(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    def get(self):
        params = {
            "created": {"gte": self.start, "lte": self.end},
            "limit": 100,
        }
        expand = ["data.source"]
        results = stripe.BalanceTransaction.list(**params, expand=expand)
        rows = [i.to_dict_recursive() for i in results.auto_paging_iter()]
        return rows

    def transform(self, rows):
        return [self._transform_to_string(row) for row in rows]

    def _transform_to_string(self, row):
        if row.get("source"):
            row["source"] = json.dumps(row["source"])
        return row

    def load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}.{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

    def update(self):
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table}
        AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {','.join(self.keys.get('p_key'))}) AS row_num
            FROM {DATASET}.{self.table}
        )
        WHERE row_num = 1
        """
        BQ_CLIENT.query(query).result()

    def run(self):
        rows = self.get()
        responses = {
            "start": self.start,
            "end": self.end,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses["output_rows"] = loads.output_rows
        return responses
