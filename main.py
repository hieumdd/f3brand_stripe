import json
import base64

from models import Stripe


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    job = Stripe.factory(
        data["resource"],
        data.get("start"),
        data.get("end"),
    )
    responses = {
        "pipelines": "Stripe",
        "results": job.run(),
    }

    print(responses)
    return responses
