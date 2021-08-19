from models import Stripe


def main(request):
    request_json = request.get_json()
    job = Stripe.factory(
        request_json["resource"],
        request_json.get("start"),
        request_json.get("end"),
    )
    responses = {
        "pipelines": "Stripe",
        "results": job.run(),
    }
    print(responses)
    return responses
