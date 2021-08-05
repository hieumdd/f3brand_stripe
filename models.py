import os
import json
from datetime import datetime
from abc import ABC, abstractmethod

import stripe
from google.cloud import bigquery

DATE_FORMAT = "%Y-%m-%d"
NOW = datetime.utcnow()

stripe.api_key = os.getenv("API_KEY")

DATASET = "stripe"
BQ_CLIENT = bigquery.Client()


class Stripe(ABC):
    def __init__(self, start, end):
        self.start, self.end = self._get_time_range(start, end)
        self.keys, self.schema = self._get_config()

    @staticmethod
    def factory(resource, start, end):
        args = (start, end)
        if resource == "BalanceTransactions":
            return BalanceTransactions(*args)
        elif resource == "Charge":
            return Charge(*args)

    def _get_time_range(self, start, end):
        if start and end:
            start, end = [datetime.strptime(i, DATE_FORMAT) for i in [start, end]]
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
                start = datetime(2021, 1, 1)
            end = NOW

        return start, end

    def _get_config(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def transform(self, rows):
        pass

    def load(self, rows):
        # with open("xxxx.json", "w") as f:
        #     json.dump(rows, f)
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
                SELECT *, ROW_NUMBER() OVER
                (PARTITION BY {','.join(self.keys.get('p_key'))}) AS row_num
                FROM {DATASET}.{self.table}
            )
            WHERE row_num = 1
        """
        BQ_CLIENT.query(query).result()

    def run(self):
        rows = self.get()
        responses = {
            "start": self.start.strftime(DATE_FORMAT),
            "end": self.end.strftime(DATE_FORMAT),
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses["output_rows"] = loads.output_rows
        return responses


class BalanceTransactions(Stripe):
    table = "BalanceTransactions"

    def get(self):
        params = {
            "created": {
                "gte": int(self.start.timestamp()),
                "lte": int(self.end.timestamp()),
            },
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


class Charge(Stripe):
    table = "Charge"

    def __init__(self, start, end):
        super().__init__(start, end)

    def get(self):
        params = {
            "created": {
                "gte": int(self.start.timestamp()),
                "lte": int(self.end.timestamp()),
            },
            "limit": 100,
        }
        expand = ["data.source"]
        results = stripe.Charge.list(**params, expand=expand)
        rows = [i.to_dict_recursive() for i in results.auto_paging_iter()]
        return rows

    def transform(self, rows):
        return [self._transform_to_string(row) for row in rows]

    def _transform_to_string(self, row):
        for i in [
            "metadata",
            "payment_method_details",
            "amount_updates",
            "source",
            "outcome",
            "refunds",
            "transfer_data",
        ]:
            row[i] = json.dumps(row.get(i, None))
        return row
