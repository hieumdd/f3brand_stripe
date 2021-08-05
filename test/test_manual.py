from .utils import process

START = "2021-07-31"
END = "2021-08-04"


def test_balance_transactions():
    data = {
        "resource": "BalanceTransactions",
        "start": START,
        "end": END,
    }
    process(data)


def test_charge():
    data = {
        "resource": "Charge",
        "start": START,
        "end": END,
    }
    process(data)
