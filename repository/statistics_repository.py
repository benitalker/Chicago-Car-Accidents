from pymongo import ASCENDING

from database.connect import get_injury_statistics_by_area_collection

def get_injury_statistics_by_area(area):
    collection = get_injury_statistics_by_area_collection()

    # Measure time without index
    no_index_explain = collection.find({"area": area}).hint({"$natural": 1}).explain()["executionStats"]
    print(f"Query without index took {no_index_explain['executionTimeMillis']} ms")
    print(f"Total docs examined without index: {no_index_explain['totalDocsExamined']}")

    # Ensure index on 'area' field
    collection.create_index([("area", ASCENDING)])

    # Measure time with index
    with_index_explain = collection.find({"area": area}).hint({"area": 1}).explain()["executionStats"]

    print(f"Query with index took {with_index_explain['executionTimeMillis']} ms")
    print(f"Total docs examined with index: {with_index_explain['totalDocsExamined']}")

    # Return the actual query result
    result = collection.find_one({"area": area})
    if result:
        return {
            "area": result["area"],
            "total_injuries": result["total_injuries"],
            "fatal_injuries": result["fatal_injuries"],
            "non_fatal_injuries": result["non_fatal_injuries"],
            "events": result["events"]
        }
    else:
        return None
