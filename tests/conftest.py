import pytest
from pymongo import MongoClient

@pytest.fixture(scope="function")
def mongodb_client():
    client = MongoClient('mongodb://172.23.14.159:27017')
    yield client
    client.close()


@pytest.fixture(scope="function")
def accidents_day_collection(mongodb_client):
    db = mongodb_client['chicago_car_accidents']
    return db['accidents_by_day']

@pytest.fixture(scope="function")
def accidents_week_collection(mongodb_client):
    db = mongodb_client['chicago_car_accidents']
    return db['accidents_by_week']

@pytest.fixture(scope="function")
def accidents_month_collection(mongodb_client):
    db = mongodb_client['chicago_car_accidents']
    return db['accidents_by_month']

@pytest.fixture(scope="function")
def accidents_cause_collection(mongodb_client):
    db = mongodb_client['chicago_car_accidents']
    return db['accidents_by_cause']

@pytest.fixture(scope="function")
def accidents_area_collection(mongodb_client):
    db = mongodb_client['chicago_car_accidents']
    return db['accidents_by_area']


@pytest.fixture(scope='function')
def injury_statistics_by_area_collection(mongodb_client):
   db = mongodb_client['chicago_car_accidents']
   return db['injury_statistics_by_area']