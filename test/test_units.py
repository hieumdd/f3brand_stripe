from unittest.mock import Mock

from main import main

def assertion(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    for i in res['results']:
        assert i['num_processed'] >= 0
        if i['num_processed'] > 0:
            assert i['num_processed'] == i['output_rows']

def test_auto():
    data = {}
    assertion(data)

def test_manual():
    data = {
        "start": "2021-07-31",
        "end": "2021-08-04"
    }
    assertion(data)
