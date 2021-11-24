from controller.pipelines import factory, run

DATASET = "stripe"


def main(request) -> dict:
    data: dict = request.get_json()
    print(data)

    if "table" in data:
        responses = run(
            DATASET,
            factory(data["table"]),
            data,
        )
        print(responses)
        return responses
    else:
        raise ValueError(data)
