from .utils import process

START = "2021-08-01"
END = "2021-08-18"


def test_balance_transaction():
    data = {
        "resource": "BalanceTransaction",
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

def test_customer():
    data = {
        "resource": "Customer",
        "start": START,
        "end": END,
    }
    process(data)
