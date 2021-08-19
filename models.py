import os
import json
from datetime import datetime, timezone
from abc import ABC, abstractmethod

import stripe
from google.cloud import bigquery

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

stripe.api_key = os.getenv("API_KEY")

DATASET = "stripe"
BQ_CLIENT = bigquery.Client()


class Stripe(ABC):
    def __init__(self, start, end):
        self.keys, self.schema = self.get_config()
        self.start, self.end = self.get_time_range(start, end)

    @staticmethod
    def factory(resource, start, end):
        args = (start, end)
        if resource == "BalanceTransaction":
            return BalanceTransaction(*args)
        elif resource == "Charge":
            return Charge(*args)
        elif resource == "Customer":
            return Customer(*args)
        else:
            raise NotImplementedError(resource)

    @property
    @abstractmethod
    def table(self):
        pass

    @property
    def params(self):
        return {
            "created": {
                "gte": self.start,
                "lte": self.end,
            },
            "limit": 100,
        }

    @property
    @abstractmethod
    def expand(self):
        pass

    def get_time_range(self, start, end):
        if start and end:
            start, end = [
                int(
                    datetime.strptime(i, DATE_FORMAT)
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                )
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
                start = datetime(2021, 1, 1)
            end = int(NOW.timestamp())

        return start, end

    def get_config(self):
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
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
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
                FROM {DATASET}._stage_{self.table}
            )
            WHERE row_num = 1
            """
        BQ_CLIENT.query(query)

    def run(self):
        rows = self.get()
        rows = [i.to_dict_recursive() for i in rows.auto_paging_iter()]
        responses = {
            "start": datetime.fromtimestamp(self.start).isoformat(timespec="seconds"),
            "end": datetime.fromtimestamp(self.end).isoformat(timespec="seconds"),
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses["output_rows"] = loads.output_rows
        return responses


class BalanceTransaction(Stripe):
    def __init__(self, start, end):
        super().__init__(start, end)

    @property
    def table(self):
        return "BalanceTransaction"

    @property
    def expand(self):
        return ["data.source"]

    def get(self):
        rows = stripe.BalanceTransaction.list(
            **self.params,
            expand=self.expand,
        )
        return rows

    def transform(self, rows):
        return [self._transform_to_string(row) for row in rows]

    def _transform_to_string(self, row):
        if row.get("source"):
            row["source"] = json.dumps(row["source"])
        return row


class Charge(Stripe):
    def __init__(self, start, end):
        super().__init__(start, end)

    @property
    def table(self):
        return "Charge"

    @property
    def expand(self):
        return ["data.source"]

    def get(self):
        rows = stripe.Charge.list(
            **self.params,
            expand=self.expand,
        )
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


class Customer(Stripe):
    def __init__(self, start, end):
        super().__init__(start, end)

    @property
    def table(self):
        return "Customer"

    @property
    def expand(self):
        return []

    def get(self):
        rows = stripe.Customer.list(
            **self.params,
            expand=self.expand,
        )
        return rows

    def transform(self, _rows):
        rows = [
            {
                "id": row["id"],
                "object": row["object"],
                "created": row["created"],
                "name": row["name"],
                "email": row["email"],
            }
            for row in _rows
        ]
        return rows
