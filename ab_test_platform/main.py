import sys
from os.path import abspath
import argparse

try:
    from ab_test import Test
except Exception as e:
    from .ab_test import Test

try:
    from utils import kill_process_with_name, url_string, get_result_file_name
except Exception as e:
    from .utils import kill_process_with_name, url_string, get_result_file_name


def main(test_groups, groups=None, date=None, feature=None, data_source=None,
         data_query_path=None, time_period=None, time_indicator=None, export_path=None, parameters=None):
    print("received :", {'test_groups': test_groups, 'groups': groups, 'date': date,
                         'feature': feature, 'data_source': data_source,
                         'data_query_path': data_query_path, 'time_period': time_period,
                         "time_indicator": time_indicator, "export_path": export_path, "parameters": parameters}
          )
    print(Test.__init__)
    ab_test = Test(test_groups=test_groups, groups=groups, date=date, feature=feature,
                     data_source=data_source, data_query_path=url_string(data_query_path),
                     time_period=time_period, time_indicator=time_indicator, export_path=export_path, parameters=parameters)
    ab_test.execute()
    print("Done!!")
    if export_path is None:
        return ab_test
    else:
        print("exporting path :", export_path)
        print(ab_test.final_results.head())
        ab_test.final_results.to_csv(get_result_file_name(export_path, date, time_period), index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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
    parser.add_argument("-TP", "--time_period", type=str,
                        help="""
                        This shows us to the time period if AB Test is running sequentially
                        """,
                        )
    parser.add_argument("-EP", "--export_path", type=str,
                        help="""
                        This shows us to the time period if AB Test is running sequentially
                        """,
                        )

    parser.add_argument("-P", "--parameters", type=str,
                        help="""
                        send test parameter (e.g Confidence interval)
                        """,
                        )
    arguments = parser.parse_args()
    args = {'test_groups': arguments.test_groups, 'groups': arguments.groups, 'date': arguments.date,
            'feature': arguments.feature, 'data_source': arguments.data_source,
            'data_query_path': arguments.data_query_path, 'time_period': arguments.time_period,
            'time_indicator': arguments.time_indicator,
            'export_path': arguments.export_path, "parameters":arguments.parameters}
    print(args)
    main(**args)
