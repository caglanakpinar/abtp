import schedule
import datetime
import time
import argparse
from dateutil.parser import parse
from os.path import abspath, dirname
from os import listdir

try:
    from main import main
    from utils import date_part
except Exception as e:
    from .main import main
    from .utils import date_part


def get_schedule(time_period):
    if time_period not in ['minute', 'hour']:
        return {'Mondays': schedule.every().monday,
                'Tuesdays': schedule.every().tuesday,
                'Wednesdays': schedule.every().wednesday,
                'Thursdays': schedule.every().thursday,
                'Fridays': schedule.every().friday,
                'Saturdays': schedule.every().saturday,
                'Sundays': schedule.every().sunday,
                'Daily': schedule.every().day
                }[time_period]
    if time_period == 'week':
        return schedule.every().week
    if time_period == 'hour':
        return schedule.every(1).hours.at()


def update_date():
    return str(datetime.datetime.now())[0:10]


def run_ab_test():
    args['date'] = update_date()
    main(**args)


def create_job(ab_test_arguments, time_period):
    global args
    args = ab_test_arguments
    _sch = get_schedule(time_period)
    _sch.do(run_ab_test)
    while True:
        _sch.run_pending()
        time.sleep(1000)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-TS", "--time_schedule", type=str,
                        help="""
                        column of the data which represents  A - B Test of groups. 
                        It  is a column name from the data.
                        AB test runs as control  - active group name related to columns of unique values.
                        This column has to 2 unique values which shows us the test groups
                        """)
    parser.add_argument("-TG", "--test_groups", type=str,
                        help="""
                        column of the data which represents  A - B Test of groups. 
                        It  is a column name from the data.
                        AB test runs as control  - active group name related to columns of unique values.
                        This column has to 2 unique values which shows us the test groups
                        """,
                        required=True)
    parser.add_argument("-G", "--groups", type=str,
                        help="""
                        column of the data which represents  individual groups for Testing. 
                        AB Testing will be applied for each Groups which are unique value of groups column in data.
                        """,
                        required=True)
    parser.add_argument("-D", "--date", type=str,
                        help="""
                        This parameter represent calulating dates. 
                        If there is time schedule for AB Test, according to date column condintion will be
                        "< = date". All data before the given date (given date is included) 
                        must be collected for test results.
                        """,
                        )
    parser.add_argument("-F", "--feature", type=str,
                        help="""
                        Represents testing values of Test. Test calculation will be applied according the feature column
                        """,
                        required=True)
    parser.add_argument("-DS", "--data_source", type=str,
                        help="""
                        AWS RedShift, BigQuery, PostgreSQL, csv, json files can be connected to system
                        """,
                        required=True)
    parser.add_argument("-DQP", "--data_query_path", type=str,
                        help="""
                        if there is file for data importing;
                            must be the path (e.g /../.../ab_test_raw_data.csv)
                        if there is ac- connection such as PostgreSQL / BigQuery
                            query must at the format "SELECT++++*+++FROM++ab_test_table_+++"
                        """,
                        required=True)
    parser.add_argument("-TI", "--time_indicator", type=str,
                        help="""
                        This can only be applied with date. It can be hour, day, week, week_part, quarter, year, month.
                        Individually time indicator checks the date part is significantly 
                        a individual group for data set or not.
                        If it is uses time_indicator as a  group
                        """,
                        )
    parser.add_argument("-EP", "--export_path", type=str,
                        help="""
                        """,
                        )
    arguments = parser.parse_args()
    ab_test_arguments = {'test_groups': arguments.test_groups, 'groups': arguments.groups, 'date': arguments.date,
                         'feature': arguments.feature, 'data_source': arguments.data_source,
                         'data_query_path': arguments.data_query_path, 'time_period': arguments.time_period,
                         "export_path": arguments.export_path}

    create_job(ab_test_arguments, arguments.time_schedule)