from pymongo import MongoClient

def get_mongo_client():
    client = MongoClient('mongodb://172.20.179.219:27017')
    return client

def get_db():
    client = get_mongo_client()
    return client['chicago_car_accidents']

def get_accidents_by_area_collection(collection_name='accidents_by_area'):
    db = get_db()
    return db[collection_name]

def get_accidents_by_day_collection():
    return get_accidents_by_area_collection('accidents_by_day')

def get_accidents_by_week_collection():
    return get_accidents_by_area_collection('accidents_by_week')

def get_accidents_by_month_collection():
    return get_accidents_by_area_collection('accidents_by_month')

def get_accidents_by_cause_collection():
    db = get_db()
    return db['accidents_by_cause']

def get_injury_statistics_by_area_collection():
    db = get_db()
    return db['injury_statistics_by_area']
