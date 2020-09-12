import sys
from inspect import getargspec

from ab_test import ABTest
from logger import *
from utils import kill_process_with_name, url_string


def main(test_groups, groups=None, date=None, feature=None, data_source=None,
         data_query_path=None, time_period=None, time_indicator=None):
    sys.stdout = Logger()
    print("received :", {'test_groups': test_groups, 'groups': groups, 'date': date,
                         'feature': feature, 'data_source': data_source,
                         'data_query_path': data_query_path, 'time_period': time_period},
          " time :", get_time()
          )
    ab_test = ABTest(test_groups=test_groups, groups=groups, date=date, feature=feature,
                     data_source=data_source, data_query_path=url_string(data_query_path),
                     time_period=time_period, time_indicator=time_indicator)
    ab_test.execute()
    get_time()
    print("Done!!")


if __name__ == '__main__':
    args = {arg: None for arg in getargspec(main)[0]}
    counter = 1
    for k in args:
        args[k] = sys.argv[counter] if sys.argv[counter] not in ['-', None] else None
        counter += 1
    print(args)
    main(**args)
