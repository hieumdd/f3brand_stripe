from .utils import process


def test_balance_transactions():
    data = {
        "resource": "BalanceTransactions",
    }
    process(data)


def test_charge():
    data = {
        "resource": "Charge",
    }
    process(data)
