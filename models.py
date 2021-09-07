import os
import json
from datetime import datetime, timezone
from abc import ABC, abstractmethod

import stripe
from google.cloud import bigquery

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

stripe.api_key = os.getenv("API_KEY")

DATASET = "stripe"
BQ_CLIENT = bigquery.Client()


class Stripe(ABC):
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
    @abstractmethod
    def stripe_obj(self):
        pass

    @property
    @abstractmethod
    def expand(self):
        pass

    def __init__(self, start, end):
        self.start, self.end = self.get_time_range(start, end)

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
                SELECT UNIX_SECONDS(MAX(created)) AS max_incre
                FROM {DATASET}.{self.table}
                """
            try:
                results = BQ_CLIENT.query(query).result()
                max_incre = [dict(row.items()) for row in results][0]["max_incre"]
                start = max_incre
            except:
                start = datetime(2021, 1, 1)
            end = int(NOW.timestamp())
        return start, end

    def _get(self):
        rows = self.stripe_obj.list(
            {
                "created": {
                    "gte": self.start,
                    "lte": self.end,
                },
                "limit": 100,
            },
            expand=self.expand,
        )
        return [i.to_dict_recursive() for i in rows.auto_paging_iter()]

    @abstractmethod
    def transform(self, rows):
        pass

    def _load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

    def _update(self):
        query = f"""
            CREATE OR REPLACE TABLE {DATASET}.{self.table}
            AS
            SELECT * EXCEPT(row_num)
            FROM (
                SELECT *, ROW_NUMBER() OVER
                (PARTITION BY id) AS row_num
                FROM {DATASET}._stage_{self.table}
            )
            WHERE row_num = 1
            """
        BQ_CLIENT.query(query)

    def run(self):
        rows = self._get()
        responses = {
            "table": self.table,
            "start": datetime.fromtimestamp(self.start).isoformat(timespec="seconds"),
            "end": datetime.fromtimestamp(self.end).isoformat(timespec="seconds"),
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self._load(rows)
            self._update()
            responses["output_rows"] = loads.output_rows
        return responses


class BalanceTransaction(Stripe):
    table = "BalanceTransaction"
    stripe_obj = stripe.BalanceTransaction
    expand = []
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "amount", "type": "INTEGER"},
        {"name": "currency", "type": "STRING"},
        {"name": "fee", "type": "INTEGER"},
        {"name": "net", "type": "INTEGER"},
        {"name": "status", "type": "STRING"},
        {"name": "type", "type": "STRING"},
        {"name": "object", "type": "STRING"},
        {"name": "available_on", "type": "TIMESTAMP"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "exchange_rate", "type": "FLOAT"},
        {"name": "reporting_category", "type": "STRING"},
    ]

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "amount": row["id"],
                "currency": row["currency"],
                "fee": row["fee"],
                "net": row["net"],
                "status": row["status"],
                "type": row["type"],
                "object": row["object"],
                "available_on": row["available_on"],
                "created": row["created"],
                "exchange_rate": row["exchange_rage"],
                "reporting_category": row["reporting_category"],
            }
            for row in rows
        ]


class Charge(Stripe):
    table = "Charge"
    stripe_obj = stripe.Charge
    expand = []
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "amount", "type": "INTEGER"},
        {
            "name": "billing_details",
            "type": "record",
            "fields": [
                {
                    "name": "address",
                    "type": "record",
                    "fields": [
                        {"name": "city", "type": "STRING"},
                        {"name": "country", "type": "STRING"},
                        {"name": "line1", "type": "STRING"},
                        {"name": "line2", "type": "STRING"},
                        {"name": "postal_code", "type": "STRING"},
                        {"name": "state", "type": "STRING"},
                    ],
                },
                {"name": "email", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
            ],
        },
        {"name": "currency", "type": "STRING"},
        {"name": "customer", "type": "STRING"},
        {"name": "disputed", "type": "BOOLEAN"},
        {"name": "invoice", "type": "STRING"},
        {"name": "receipt_email", "type": "STRING"},
        {"name": "refunded", "type": "BOOLEAN"},
        {"name": "status", "type": "STRING"},
        {"name": "object", "type": "STRING"},
        {"name": "amount_captured", "type": "INTEGER"},
        {"name": "amount_refunded", "type": "INTEGER"},
        {"name": "captured", "type": "BOOLEAN"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "order", "type": "STRING"},
        {"name": "paid", "type": "BOOLEAN"},
        {"name": "dispute", "type": "STRING"},
    ]

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "amount": row["amount"],
                "billing_details": {
                    "address": {
                        "city": row["billing_details"].get("address").get("city"),
                        "country": row["billing_details"].get("address").get("country"),
                        "line1": row["billing_details"].get("address").get("line1"),
                        "line2": row["billing_details"].get("address").get("line2"),
                        "postal_code": row["billing_details"]
                        .get("address")
                        .get("postal_code"),
                        "state": row["billing_details"].get("address").get("state"),
                    },
                    "email": row["billing_details"].get("email"),
                    "name": row["billing_details"].get("name"),
                    "phone": row["billing_details"].get("phone"),
                },
                "currency": row["currency"],
                "customer": row["customer"],
                "disputed": row["disputed"],
                "invoice": row["invoice"],
                "receipt_email": row["receipt_email"],
                "refunded": row["refunded"],
                "status": row["status"],
                "object": row["object"],
                "amount_captured": row["amount_captured"],
                "amount_refunded": row["amount_refunded"],
                "captured": row["captured"],
                "created": row["created"],
                "order": row["order"],
                "paid": row["paid"],
                "dispute": row["dispute"],
            }
            for row in rows
        ]


class Customer(Stripe):
    table = "Customer"
    stripe_obj = stripe.Customer
    expand = []
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "object", "type": "STRING"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "name", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {
            "name": "metadata",
            "type": "RECORD",
            "fields": [
                {"name": "kjb_member_id", "type": "STRING"},
                {"name": "street_line_1", "type": "STRING"},
                {"name": "street_line_2", "type": "STRING"},
                {"name": "city", "type": "STRING"},
                {"name": "country", "type": "STRING"},
                {"name": "region", "type": "STRING"},
                {"name": "postal_code", "type": "STRING"},
                {"name": "phone_number", "type": "STRING"},
            ],
        },
    ]

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "object": row["object"],
                "created": row["created"],
                "name": row["name"],
                "email": row["email"],
                "metadata": {
                    "kjb_member_id": row["metadata"].get("kjb_member_id"),
                    "street_line_1": row["metadata"].get("street_line_1"),
                    "street_line_2": row["metadata"].get("street_line_2"),
                    "city": row["metadata"].get("city"),
                    "country": row["metadata"].get("country"),
                    "region": row["metadata"].get("region"),
                    "postal_code": row["metadata"].get("postal_code"),
                    "phone_number": row["metadata"].get("phone_number"),
                }
                if row.get("metadata")
                else {},
            }
            for row in rows
        ]
