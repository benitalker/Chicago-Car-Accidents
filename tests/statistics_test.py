from repository.statistics_repository import get_injury_statistics_by_area


def test_get_injury_statistics_by_area(injury_statistics_by_area_collection):
    area = '1822'

    # Call the function
    result = get_injury_statistics_by_area(area)

    # Assertions
    assert result is not None, f"Expected non-empty result for area {area}"
    assert result["area"] == area, f"Expected area to be {area}"
    assert "total_injuries" in result, "Expected 'total_injuries' in result"
    assert "fatal_injuries" in result, "Expected 'fatal_injuries' in result"
    assert "non_fatal_injuries" in result, "Expected 'non_fatal_injuries' in result"
    assert "events" in result, "Expected 'events' in result"
    assert isinstance(result["events"], list), "Expected 'events' to be a list"
