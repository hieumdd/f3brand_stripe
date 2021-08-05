from models import BalanceTransactions

def main(request):
    request_json = request.get_json()
    job = BalanceTransactions(start=request_json.get('start'), end=request_json.get('end'))
    responses = {
        "pipelines": "Stripe",
        "results": [job.run()]
    }
    print(responses)
    return responses
