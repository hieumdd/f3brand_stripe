import pytest

from .utils import process


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
    process(data)
