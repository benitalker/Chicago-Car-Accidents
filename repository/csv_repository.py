import os
import time

from pymongo import ASCENDING

from database.connect import get_accidents_by_area_collection, get_injury_statistics_by_area_collection, \
    get_accidents_by_cause_collection, get_accidents_by_month_collection, get_accidents_by_week_collection, \
    get_accidents_by_day_collection
from service.csv_service import aggregate_by_day_week_month, aggregate_by_cause, aggregate_injury_statistics, \
    aggregate_total_accidents_by_area

# Insert aggregated data into collections and create indexes for efficient queries
def insert_accidents_by_day(data_by_day):
    collection = get_accidents_by_day_collection()
    for (area, day), data in data_by_day.items():
        collection.insert_one({
            "area": area,
            "day": day,
            "total_accidents": data["total_accidents"],
            "injuries": data["injuries"],
            "contributing_factors": dict(data["contributing_factors"])
        })
    # Create indexes for optimized queries
    collection.create_index([("area", ASCENDING), ("day", ASCENDING)])

def insert_accidents_by_week(data_by_week):
    collection = get_accidents_by_week_collection()
    for (area, week_start, week_end), data in data_by_week.items():
        collection.insert_one({
            "area": area,
            "week_start": week_start,
            "week_end": week_end,
            "total_accidents": data["total_accidents"],
            "injuries": data["injuries"],
            "contributing_factors": dict(data["contributing_factors"])
        })
    # Create indexes for optimized queries
    collection.create_index([("area", ASCENDING), ("week_start", ASCENDING), ("week_end", ASCENDING)])

def insert_accidents_by_month(data_by_month):
    collection = get_accidents_by_month_collection()
    for (area, month), data in data_by_month.items():
        collection.insert_one({
            "area": area,
            "month": month,
            "total_accidents": data["total_accidents"],
            "injuries": data["injuries"],
            "contributing_factors": dict(data["contributing_factors"])
        })
    # Create indexes for optimized queries
    collection.create_index([("area", ASCENDING), ("month", ASCENDING)])

def insert_accidents_by_cause(data_by_cause):
    collection = get_accidents_by_cause_collection()
    for area, causes in data_by_cause.items():
        collection.insert_one({
            "area": area,
            "causes": dict(causes)
        })
    # Create index on the area field for efficient queries
    collection.create_index([("area", ASCENDING)])

def insert_injury_statistics(data_by_area):
    collection = get_injury_statistics_by_area_collection()
    for area, stats in data_by_area.items():
        collection.insert_one({
            "area": area,
            "total_injuries": stats["total_injuries"],
            "fatal_injuries": stats["fatal_injuries"],
            "non_fatal_injuries": stats["non_fatal_injuries"],
            "events": stats["events"]
        })
    # Create index on the area field for efficient queries
    collection.create_index([("area", ASCENDING)])

def insert_total_accidents_by_area(data_by_area):
    collection = get_accidents_by_area_collection()
    for area, total_accidents in data_by_area.items():
        collection.insert_one({
            "area": area,
            "total_accidents": total_accidents
        })
    # Create index on the area field for efficient queries
    collection.create_index([("area", ASCENDING)])

# Run all aggregation functions and initialize the database with indexing
def initialize_database():
    start_time = time.time()
    # Get the current directory of the script
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Build the full path to the CSV file
    csv_file_path = os.path.join(current_dir, '../data/data.csv')
    # csv_file_path = os.path.join(current_dir, '../data/Traffic_Crashes_Crashes.csv')

    # Aggregate data by day, week, month
    data_by_day, data_by_week, data_by_month = aggregate_by_day_week_month(csv_file_path)
    insert_accidents_by_day(data_by_day)
    insert_accidents_by_week(data_by_week)
    insert_accidents_by_month(data_by_month)

    # Aggregate accidents by cause
    data_by_cause = aggregate_by_cause(csv_file_path)
    insert_accidents_by_cause(data_by_cause)

    # Aggregate injury statistics
    injury_statistics = aggregate_injury_statistics(csv_file_path)
    insert_injury_statistics(injury_statistics)

    # Aggregate total accidents by area
    total_accidents_by_area = aggregate_total_accidents_by_area(csv_file_path)
    insert_total_accidents_by_area(total_accidents_by_area)
    time_no_index = time.time() - start_time

    print("Database initialization complete with aggregated data and indexing.")
