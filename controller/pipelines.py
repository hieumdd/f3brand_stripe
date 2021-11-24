import importlib

from models.stripe import StripePipeline


def factory(table: str) -> StripePipeline:
    try:
        return getattr(importlib.import_module(f"models.stripe"), table)
    except (ImportError, AttributeError):
        raise ValueError(table)


def run(dataset: str, pipeline: StripePipeline, request_data: dict) -> dict:
    return pipeline(dataset, request_data.get("start"), request_data.get("end"))
