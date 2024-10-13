import time
from pymongo import ASCENDING
from bson import ObjectId, json_util


def convert_object_id(data):
    if isinstance(data, list):
        return [convert_object_id(item) for item in data]
    elif isinstance(data, dict):
        return {key: (str(value) if isinstance(value, ObjectId) else value) for key, value in data.items()}
    else:
        return data


