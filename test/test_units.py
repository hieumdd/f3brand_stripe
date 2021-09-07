import json
import base64
from unittest.mock import Mock

import pytest

from main import main


@pytest.mark.parametrize(
    "resource",
    [
        "BalanceTransaction",
        "Charge",
        "Customer",
    ],
)
@pytest.mark.parametrize(
    "start,end",
    [
        (None, None),
        ("2021-07-01", "2021-09-01"),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
@pytest.mark.timeout(0)
def test_manual(resource, start, end):
    data = {
        "resource": resource,
        "start": start,
        "end": end,
    }
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    message = {
        "message": {
            "data": data_encoded,
        },
    }
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    results = res["results"]
    assert results["num_processed"] >= 0
    if results["num_processed"] > 0:
        assert results["num_processed"] == results["output_rows"]
