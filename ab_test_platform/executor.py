from os.path import join
import threading

try:
    from main import main
    from data_access import GetData
    from utils import get_folder_path, write_yaml, read_yaml
    from configs import conf
    from scheduler_service import create_job
except Exception as e:
    from .main import main
    from .data_access import GetData
    from .utils import get_folder_path, write_yaml, read_yaml
    from .configs import conf
    from .scheduler_service import create_job


class ABTest:
    """
    test_groups:        column of the data which represents  A - B Test of groups.
                        It  is a column name from the data.
                        AB test runs as control  - active group name related to columns of unique values.
                        This column has to 2 unique values which shows us the test groups

    groups:             column of the data which represents  individual groups for Testing.
                        AB Testing will be applied for each Groups which are unique value of groups column in data.

    feature:            Represents testing values of Test.
                        Test calculation will be applied according the feature column

    data_source:        AWS RedShift, BigQuery, PostgreSQL, csv, json files can be connected to system
                        E.g.
                        {"data_source": ..., "db": ..., "password": ..., "port": ..., "server": ..., "user": ...}

    data_query_path:    if there is file for data importing;
                            must be the path (e.g /../.../ab_test_raw_data.csv)
                        if there is ac- connection such as PostgreSQL / BigQuery
                            query must at the format "SELECT++++*+++FROM++ab_test_table_+++"

    time_indicator:     This can only be applied with date. It can be hour, day, week, week_part, quarter, year, month.
                        Individually time indicator checks the date part is significantly
                        a individual group for data set or not.
                        If it is uses time_indicator as a  group

    export_path:        Output results of export as csv format (optional).
                        only path is enough for importing data with .csv format.
                        Output will be '<date>_results.csv' with the test executed date. e.g. 20201205.results.csv

    time_schedule:      When AB Test need to be scheduled, only need to be assign here 'Hourly', 'Monthly',
                        'Weekly', 'Mondays', ... , Sundays.

    time_period:        The additional time period which (optional year, month, day, hour, week,
                        week day, day part, quarter) (check details time periods).
                        This parameter must be assigned when A/B Test is scheduling.
    """
    def __init__(self,
                 test_groups,
                 groups=None,
                 feature=None,
                 data_source=None,
                 data_query_path=None,
                 time_period=None,
                 time_indicator=None,
                 time_schedule=None,
                 export_path=None,
                 connector=None,
                 confidence_level=None,
                 boostrap_sample_ratio=None,
                 boostrap_iteration=None):
        self.test_groups = test_groups
        self.groups = groups
        self.feature = feature
        self.data_source = data_source
        self.data_query_path = data_query_path
        self.time_period = time_period
        self.time_indicator = time_indicator
        self.time_schedule = time_schedule
        self.export_path = export_path
        self.connector = connector
        self.confidence_level = confidence_level
        self.boostrap_sample_ratio = boostrap_sample_ratio
        self.boostrap_iteration = boostrap_iteration
        self.arguments = {"test_groups": test_groups,
                          "groups": groups,
                          "feature": feature,
                          "data_source": data_source,
                          "data_query_path": data_query_path,
                          "time_period": time_period,
                          "time_indicator": time_indicator,
                          "export_path": export_path,
                          "parameters": None}
        self.arg_terminal = {"test_groups": "TG",
                             "groups": "G",
                             "date": "D",
                             "feature": "F",
                             "data_source":  "DS",
                             "data_query_path": "DQP",
                             "time_period": "TP",
                             "time_indicator": "TI", "export_path": "EP", "parameters": "P"}
        self.args_str = ""
        self.ab_test = None
        self.path = get_folder_path()
        self.mandetory_arguments = ["data_source", "data_query_path", "test_groups", "groups", "feature", "export_path"]
        self.schedule_arg = "TS"
        self.params = None

    def get_connector(self):
        """
       query_string_change connection checks.
       tries for db connections (postgresql, RedShift, googlebigquery).
       If fials checks for
        """
        config = conf('config')
        try:
            data_access_args = {"data_source": self.data_source,
                                "data_query_path": self.data_query_path,
                                "time_indicator": self.time_indicator,
                                "feature": self.feature}

            for i in config['db_connection']:
                if i != 'data_source':
                    config['db_connection'][i] = None
                    if self.data_source not in ["csv", "json"]:
                        config['db_connection'][i] = self.connector[i]
                else:
                    config['db_connection']['data_source'] = self.data_source
            if self.data_source in ["csv", "json"]:
                data_access_args['test'] = 10
            write_yaml(join(self.path, "docs"), "configs.yaml", config, ignoring_aliases=False)
            source = GetData(**data_access_args)
            source.get_connection()
            if self.data_source in ["csv", "json"]:
                source.data_execute()
                return True if len(source.data) != 0 else False
            else: return True
        except Exception as e:
            return False

    def query_string_change(self):
        if self.data_source in ['mysql', 'postgresql', 'awsredshift', 'googlebigquery']:
            self.data_query_path = self.data_query_path.replace("\r", " ").replace("\n", " ").replace(" ", "+") + "+"

    def check_for_time_period(self):
        if self.time_period is None:
            return True
        else:
            if self.time_period in ["day", "year", "month", "week", "week_day",
                                    "hour", "quarter", "week_part", "day_part"]:
                return True
            else:
                return False

    def check_for_time_schedule(self):
        if self.time_schedule is None:
            return True
        else:
            if self.time_schedule in ["Mondays", "Tuesdays", "Wednesdays", "Thursdays", "Fridays",
                                      "Saturdays", "Sundays", "Daily", "hour", "week"]:
                return True
            else:
                return False

    def assign_test_parameters(self, param, param_name):
        if param is not None:
            for i in self.params:
                if type(param) == list:
                    if len([i for i in param if 0 < i < 1]) != 0:
                        self.params[i][param_name] = "_".join([str(i) for i in param if 0 < i < 1])
                else:
                    if 0 < param < 1:
                        self.params[i][param_name] = str(param)

    def check_for_test_parameters(self):
        """
        checking and updating test parameters; confidence_level and boostrap_ratio
        Boostrap Ratio: decision of the ratio of boostraping.
                        sample_size = data_size * boostrap_Ratio
        Confidence Level: The decision of Hypothesis Test of Confidences Level.
                          This allows us to run or A/B Test with more than one Confidence Level to see
                          how the Hypothesis of Acceptance is changing.
        """
        if self.confidence_level is not None or self.boostrap_sample_ratio is not None:
            self.params = read_yaml(join(self.path, "docs"), "test_parameters.yaml")['test_parameters']
            for _p in [(self.confidence_level, "confidence_level"),
                       (self.boostrap_sample_ratio, "sample_size"),
                       (self.boostrap_iteration, "iteration")]:
                self.assign_test_parameters(param=_p[0], param_name=_p[1])
            self.arguments["parameters"] = self.params

    def check_for_mandetory_arguments(self):
        for arg in self.arg_terminal:
            if arg in self.mandetory_arguments:
                return False if self.arguments[arg] is None else True

    def ab_test_init(self):
        """
        Initialize A/B Test. After assgn parameters don`t forget to run ab_test_init.
        Example;

        groups = "groups"
        test_groups = "test_groups"
        feature = "feature"
        data_source = "postgresql"
        connector = {"user": ***, "password": ***, "server": "127.0.0.1",
        "port": ****, "db": ***}
        data_main_path = '
                        SELECT
                        groups,
                        test_groups
                        feature,
                        time_indicator
                        FROM table
        '
        confidence_level = [0.01, 0.05]
        boostrap_ratio = [0.1, 0.2]
        export_path =  abspath("") + '/data'

        ab = ABTest(test_groups=test_groups,
                groups=groups,
                feature=feature,
                data_source=data_source,
                data_query_path=query,
                time_period=time_period,
                time_indicator=time_indicator,
                time_schedule=time_schedule,
                export_path=export_path,
                connector=connector,
                confidence_level=confidence_level,
                boostrap_sample_ratio=boostrap_ratio)
        ab.ab_test_init()
        """
        self.check_for_test_parameters()  # checking and updating test parameters; confidence_level and boostrap_ratio
        self.query_string_change()
        if self.get_connector():  # connection to data source check
            if self.check_for_time_period():
                if self.check_for_mandetory_arguments():
                    self.ab_test = main(**self.arguments)
                else:
                    print("check for the required paramters to initialize A/B Test:")
                    print(" - ".join(self.mandetory_arguments))
            else:
                print("optional time periods are :")
                print("year", "month", "week", "week_day", "hour", "quarter", "week_part", "day_part")
        else:
            print("pls check for data source connection / path / query.")

    def schedule_test(self):
        """
        schedule A/B Test with given time periods.
        """
        if self.get_connector():
            if self.check_for_time_schedule():
                if self.check_for_mandetory_arguments():
                    process = threading.Thread(target=create_job, kwargs={'ab_test_arguments': self.arguments,
                                                                          'time_period': self.time_schedule})
                    process.daemon = True
                    process.start()
                else:
                    print("check for the required parameters to initialize A/B Test:")
                    print(" - ".join(self.mandetory_arguments))

            else:
                print("optional schedule time periods are :")
                print("Mondays - .. - Sundays", "Daily", "week", "hour")
        else:
            print("pls check for data source connection / path / query.")

    def show_dashboard(self):
        """
        if you are running dashboard make sure you have assigned export_path.
        """

