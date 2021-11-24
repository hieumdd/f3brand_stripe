from unittest.mock import Mock

import pytest

from main import main


@pytest.mark.parametrize(
    "table",
    [
        "Charge",
        "Customer",
    ],
)
@pytest.mark.parametrize(
    "start,end",
    [
        (None, None),
        ("2021-07-01", "2021-07-02"),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
@pytest.mark.timeout(0)
def test_unit(table, start, end):
    data = {
        "table": table,
        "start": start,
        "end": end,
    }
    res = main(Mock(get_json=Mock(return_value=data), args=data))
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]
