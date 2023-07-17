import pandas as pd
from datetime import datetime, timedelta


# List of specific Korean Stock Exchange holidays in YYYY-MM-DD format.
korean_holidays = ['2023-01-23',
                   '2023-01-24',
                   '2023-03-01',
                   '2023-05-01',
                   '2023-05-05',
                   '2023-05-29',
                   '2023-06-06',
                   '2023-08-15',
                   '2023-09-28',
                   '2023-09-29',
                   '2023-10-03',
                   '2023-10-09',
                   '2023-12-25',
                   '2023-12-29',
]

# Iterate back from today's date to find the latest non-holiday weekday
def latest_korea_stock_date():
    date = datetime.today() - timedelta(days=1)

    # Convert the dates from string to datetime objects
    korean_holidays_dates = [datetime.strptime(day, '%Y-%m-%d') for day in korean_holidays]

    # Go back day by day until we find a date that is not a holiday or weekend
    while True:
        # Ignore the time component
        date = date.replace(hour=0, minute=0, second=0, microsecond=0)

        # Check if the date is a holiday
        if date in korean_holidays_dates:
            date = date - timedelta(days=1)
            continue

        # Check if the date is a weekend
        if date.weekday() >= 5:  # 0 is Monday, 1 is Tuesday, ..., 5 is Saturday, 6 is Sunday
            date = date - timedelta(days=1)
            continue

        # If the date is not a holiday or weekend, we are done
        break

    return date.strftime('%Y%m%d')



# List of dates to exclude in 'yyyymmdd' format (e.g. '20200101')

def subtract_korea_stock_date(date_str, days_to_subtract):
    # Convert input date from 'yyyymmdd' format to datetime object
    date = datetime.strptime(date_str, '%Y%m%d')

    # Create pandas date_range from 120 days ago to the input date
    dates = pd.date_range(end=date, periods=days_to_subtract+120) #add buffer for weekends and excluded dates

    # Convert excluded_dates to datetime objects
    excluded_dates_dt = [datetime.strptime(d, '%Y-%m-%d') for d in korean_holidays]

    # Filter out weekends and excluded dates
    dates = dates[~dates.isin(excluded_dates_dt)]
    dates = dates[dates.to_series().dt.dayofweek < 5] # 5 and 6 corresponds to Saturday and Sunday

    # Subtract 120 business days (excluding certain dates) from the input date
    final_date = dates[-days_to_subtract-1]

    return final_date.date().strftime('%Y%m%d')



