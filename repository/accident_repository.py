from datetime import datetime, timedelta

from pymongo import ASCENDING

from database.connect import (
    get_accidents_by_day_collection,
    get_accidents_by_week_collection,
    get_accidents_by_month_collection, get_accidents_by_cause_collection, get_accidents_by_area_collection
)

def get_accidents_by_day(area, day):
    collection = get_accidents_by_day_collection()
    get_accidents_by_day_collection().create_index([("area", ASCENDING), ("day", ASCENDING)])

    # Query without index (using '$natural' hint to force collection scan)
    no_index_execution_stats = collection.find({"area": area, "day": day}).hint({'$natural': 1}).explain()['executionStats']

    print(f"Query by day without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({"area": area, "day": day}).hint({'area': 1, 'day': 1}).explain()['executionStats']

    print(f"Query by day with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return actual query result
    return collection.find_one({"area": area, "day": day})

def get_accidents_by_week(area, start_date):
    collection = get_accidents_by_week_collection()

    # Convert the start_date string to a datetime object (ignoring time)
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = start_dt + timedelta(days=7)  # Add 7 days to get the end of the week

    no_index_execution_stats = collection.find({
        "area": area,
        "week_start": {"$gte": start_dt},
        "week_end": {"$lt": end_dt}
    }).hint({'$natural': 1}).explain()['executionStats']

    print(f"Query by week without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({
        "area": area,
        "week_start": {"$gte": start_dt},
        "week_end": {"$lt": end_dt}
    }).hint({'area': 1, 'week_start': 1, 'week_end': 1}).explain()['executionStats']

    print(f"Query by week with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return actual query result
    return collection.find_one({
        "area": area,
        "week_start": {"$gte": start_dt},
        "week_end": {"$lt": end_dt}
    })

def get_accidents_by_month(area, month):
    collection = get_accidents_by_month_collection()

    # Query without index
    no_index_execution_stats = collection.find({"area": area, "month": month}).hint({'$natural': 1}).explain()['executionStats']
    print(f"Query by month without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({"area": area, "month": month}).hint({'area': 1, 'month': 1}).explain()['executionStats']

    print(f"Query by month with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return actual query result
    return collection.find_one({"area": area, "month": month})

# Get accidents grouped by the primary cause in a specific area
def get_accidents_grouped_by_cause(area):
    collection = get_accidents_by_cause_collection()

    # Measure query performance without index
    no_index_explain = collection.find({"area": area}).hint({'$natural': 1}).explain()
    print(f"Query without index took {no_index_explain['executionStats']['executionTimeMillis']} ms")

    # Measure query performance with index
    with_index_explain = collection.find({"area": area}).hint({'area': 1}).explain()
    print(f"Query with index took {with_index_explain['executionStats']['executionTimeMillis']} ms")

    # Return the actual query result
    result = collection.find_one({"area": area})
    return result['causes'] if result else None

def get_accidents_by_area(area):
    collection = get_accidents_by_area_collection()

    # Query without index (using '$natural' hint to force a collection scan)
    no_index_execution_stats = collection.find({'area': area}).hint({'$natural': 1}).explain()['executionStats']

    # Print execution stats without index
    print(f"Query without index took {no_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_execution_stats['totalDocsExamined']}")

    # Query with index
    with_index_execution_stats = collection.find({'area': area}).hint({'area': 1}).explain()['executionStats']

    # Print execution stats with index
    print(f"Query with index took {with_index_execution_stats['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_execution_stats['totalDocsExamined']}")

    # Return the actual query result (using index)
    return list(collection.find({'area': area}))
