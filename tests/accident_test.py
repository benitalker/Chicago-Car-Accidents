from pymongo.collection import Collection

from repository.accident_repository import get_accidents_by_day, get_accidents_by_week, get_accidents_by_month, \
    get_accidents_grouped_by_cause, get_accidents_by_area

def test_get_accidents_by_day(accidents_day_collection: Collection):
    area = '1211'
    day = '2023-07-29'

    # Retrieve data using repository function
    result = get_accidents_by_day(area, day)

    assert result is not None, "Expected non-empty result for accidents by day"
    assert result['area'] == area, f"Expected area to be {area}"
    assert result['day'] == day, f"Expected day to be {day}"


def test_get_accidents_by_week(accidents_week_collection: Collection):
    area = '1211'
    week_start = '2023-07-24'
    week_end = '2023-07-30'

    # Retrieve data using repository function
    result = get_accidents_by_week(area, week_start, week_end)

    assert result is not None, "Expected non-empty result for accidents by week"
    assert result['area'] == area, f"Expected area to be {area}"
    assert result['week_start'].strftime('%Y-%m-%d') == week_start, f"Expected week_start to be {week_start}"
    assert result['week_end'].strftime('%Y-%m-%d') == week_end, f"Expected week_end to be {week_end}"


def test_get_accidents_by_month(accidents_month_collection: Collection):
    area = '1211'
    month = '2023-07'

    # Retrieve data using repository function
    result = get_accidents_by_month(area, month)

    assert result is not None, "Expected non-empty result for accidents by month"
    assert result['area'] == area, f"Expected area to be {area}"
    assert result['month'] == month, f"Expected month to be {month}"


def test_get_accidents_grouped_by_cause(accidents_cause_collection: Collection):
    area = '1211'

    # Retrieve data using repository function
    result = get_accidents_grouped_by_cause(area)

    assert len(result) > 0, "Expected non-empty result for accidents grouped by cause"
    for cause in result:
        assert 'total_accidents' in cause, "Expected total_accidents in result"
        assert '_id' in cause, "Expected cause ID (_id) in result"


def test_get_accidents_by_area(accidents_area_collection: Collection):
    area = '1211'

    # Retrieve data using repository function
    result = get_accidents_by_area(area)

    assert len(result) > 0, "Expected non-empty result for accidents by area"
    for accident in result:
        assert accident['area'] == area, f"Expected area to be {area}"
