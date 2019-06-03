from datetime import date
from datetime import datetime


def get_current_date_in_string():
    today = str(date.today())
    return today


def compare_dates(first_date_string, second_date_string):
    first_date = datetime.strptime(first_date_string, "%Y-%m-%d")
    second_date = datetime.strptime(second_date_string, '%Y-%m-%d')
    if first_date > second_date:
        return True
    return False

