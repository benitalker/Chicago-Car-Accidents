import csv
from collections import defaultdict
from datetime import timedelta

from utils.csv_utils import safe_int, parse_crash_date


# Read CSV and yield rows
def read_csv(csv_file_path):
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row




# Data aggregation logic for day, week, and month
def aggregate_by_day_week_month(csv_file_path):
    data_by_day = defaultdict(lambda: {"total_accidents": 0, "injuries": {"total": 0, "fatal": 0, "non_fatal": 0},
                                       "contributing_factors": defaultdict(int)})
    data_by_week = defaultdict(lambda: {"total_accidents": 0, "injuries": {"total": 0, "fatal": 0, "non_fatal": 0},
                                        "contributing_factors": defaultdict(int)})
    data_by_month = defaultdict(lambda: {"total_accidents": 0, "injuries": {"total": 0, "fatal": 0, "non_fatal": 0},
                                         "contributing_factors": defaultdict(int)})

    for row in read_csv(csv_file_path):
        crash_date = parse_crash_date(row['CRASH_DATE'])
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')

        # Daily, Weekly, Monthly aggregation
        day = crash_date.strftime('%Y-%m-%d')
        week_start = crash_date - timedelta(days=crash_date.weekday())
        week_end = week_start + timedelta(days=6)
        month = crash_date.strftime('%Y-%m')

        # Injuries and contributing factors
        injuries_total = safe_int(row.get('INJURIES_TOTAL', 0))
        injuries_fatal = safe_int(row.get('INJURIES_FATAL', 0))
        non_fatal = safe_int(row.get('INJURIES_NON_INCAPACITATING', 0))
        primary_cause = row.get('PRIM_CONTRIBUTORY_CAUSE', 'Unknown')

        # Aggregate by day
        data_by_day[(area, day)]["total_accidents"] += 1
        data_by_day[(area, day)]["injuries"]["total"] += injuries_total
        data_by_day[(area, day)]["injuries"]["fatal"] += injuries_fatal
        data_by_day[(area, day)]["injuries"]["non_fatal"] += non_fatal
        data_by_day[(area, day)]["contributing_factors"][primary_cause] += 1

        # Aggregate by week
        data_by_week[(area, week_start, week_end)]["total_accidents"] += 1
        data_by_week[(area, week_start, week_end)]["injuries"]["total"] += injuries_total
        data_by_week[(area, week_start, week_end)]["injuries"]["fatal"] += injuries_fatal
        data_by_week[(area, week_start, week_end)]["injuries"]["non_fatal"] += non_fatal
        data_by_week[(area, week_start, week_end)]["contributing_factors"][primary_cause] += 1

        # Aggregate by month
        data_by_month[(area, month)]["total_accidents"] += 1
        data_by_month[(area, month)]["injuries"]["total"] += injuries_total
        data_by_month[(area, month)]["injuries"]["fatal"] += injuries_fatal
        data_by_month[(area, month)]["injuries"]["non_fatal"] += non_fatal
        data_by_month[(area, month)]["contributing_factors"][primary_cause] += 1

    return data_by_day, data_by_week, data_by_month


# Data aggregation by cause (primary cause)
def aggregate_by_cause(csv_file_path):
    data_by_cause = defaultdict(lambda: defaultdict(int))

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        primary_cause = row.get('PRIM_CONTRIBUTORY_CAUSE', 'Unknown')

        # Count accidents by cause in each area
        data_by_cause[area][primary_cause] += 1

    return data_by_cause

def aggregate_injury_statistics(csv_file_path):
    injury_stats = defaultdict(lambda: {
        "total_injuries": 0,
        "fatal_injuries": 0,
        "non_fatal_injuries": 0,
        "events": []
    })

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        injuries_total = safe_int(row.get('INJURIES_TOTAL', 0))
        injuries_fatal = safe_int(row.get('INJURIES_FATAL', 0))
        non_fatal = safe_int(row.get('INJURIES_NON_INCAPACITATING', 0))

        # Aggregate injury statistics for the area
        injury_stats[area]['total_injuries'] += injuries_total
        injury_stats[area]['fatal_injuries'] += injuries_fatal
        injury_stats[area]['non_fatal_injuries'] += non_fatal

        # Store event details
        injury_stats[area]['events'].append({
            'crash_date': row['CRASH_DATE'],
            'injuries_total': injuries_total,
            'injuries_fatal': injuries_fatal,
            'injuries_non_fatal': non_fatal
        })

    return injury_stats


# Data aggregation by total accidents in each area
def aggregate_total_accidents_by_area(csv_file_path):
    data_by_area = defaultdict(int)

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        data_by_area[area] += 1

    return data_by_area