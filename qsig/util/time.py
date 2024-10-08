# Date & Time utils

import datetime


def date_range(date_from, date_upto):
    date = date_from
    while date < date_upto:
        yield date
        date += datetime.timedelta(days=1)
