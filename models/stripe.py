from typing import Callable, Optional
from libs.stripe import get
from libs.bigquery import get_time_range, load

StripePipeline = Callable[[str, Optional[str], Optional[str]], dict]


def stripe_pipelines(
    endpoint: str,
    expand: list[dict],
    transform: Callable[[list[dict]], list[dict]],
    schema: list[dict],
) -> StripePipeline:
    def pipeline(dataset, start, end):
        start, end = get_time_range(dataset, endpoint, start, end)
        data = get(endpoint, expand, start, end)
        responses = {
            "table": endpoint,
            "start": start.isoformat(timespec="seconds"),
            "end": end.isoformat(timespec="seconds"),
            "num_processed": len(data),
        }
        if len(data) > 0:
            responses["output_rows"] = load(dataset, endpoint, schema, transform(data))
        return responses

    return pipeline


Charge = stripe_pipelines(
    "Charge",
    [],
    lambda rows: [
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
    ],
    [
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
    ],
)

Customer = stripe_pipelines(
    "Customer",
    [],
    lambda rows: [
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
    ],
    [
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
    ],
)
