# Date & Time utils

import datetime


def date_range(date_from, date_upto):
    date = date_from
    while date < date_upto:
        yield date
        date += datetime.timedelta(days=1)

# Given a time period string such as "10m" convert that into an integer number
# of seconds.
def parse_time_period(period: str):
    period = period.strip()
    assert len(period)>=2
    assert period[0] != "0"
    if period[-1] == "m":
        return int(period[0:-1]) * 60
    elif period[-1] == "s":
        return int(period[0:-1]) * 1
    elif period[-1] == "h":
        return int(period[0:-1]) * 60 * 60
    else:
        raise Exception(f"cannot parse time period '{period}'")
