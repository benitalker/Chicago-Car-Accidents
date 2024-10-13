from datetime import datetime


def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


# Parse the crash date into a datetime object with multiple possible formats
def parse_crash_date(date_str):
    formats = [
        '%m/%d/%Y %I:%M:%S %p',  # Format with 12-hour clock, seconds, and AM/PM
        '%m/%d/%Y %I:%M %p',  # Format with 12-hour clock and AM/PM (no seconds)
        '%m/%d/%Y %H:%M',  # Format without AM/PM (24-hour time)
        '%m/%d/%Y %H:%M:%S',  # Format with seconds and 24-hour time
        '%m/%d/%Y'  # Date only
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # If none of the formats matched, raise an error
    raise ValueError(f"Time data '{date_str}' does not match any known formats.")