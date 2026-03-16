from datetime import datetime, timedelta


def generate_week_ranges(start_date, end_date):

    current = start_date

    weeks = []

    while current < end_date:

        week_start = current - timedelta(days=current.weekday())
        week_end = week_start + timedelta(days=7)

        weeks.append((week_start, week_end))

        current = week_end

    return weeks