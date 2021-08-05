from unittest.mock import Mock

from main import main

def process(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    res = res['results']
    assert res['num_processed'] >= 0
    if res['num_processed'] > 0:
        assert res['num_processed'] == res['output_rows']
