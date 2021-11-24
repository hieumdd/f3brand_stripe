import os
from datetime import datetime
from math import floor

import stripe

stripe.api_key = os.getenv("API_KEY")  # type: ignore


def get(endpoint: str, expand: list[str], start: datetime, end: datetime) -> list[dict]:
    data = getattr(stripe, endpoint).list(
        created={
            "gte": floor(start.timestamp()),
            "lte": floor(end.timestamp()),
        },
        limit=100,
        expand=expand,
    )
    return [i.to_dict_recursive() for i in data.auto_paging_iter()]
