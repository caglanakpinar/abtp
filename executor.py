from os.path import join
import subprocess

from main import main
from data_access import GetData
from utils import get_folder_path, write_yaml
from configs import conf


class ABTest:
    """
    test_groups:        column of the data which represents  A - B Test of groups.
                        It  is a column name from the data.
                        AB test runs as control  - active group name related to columns of unique values.
                        This column has to 2 unique values which shows us the test groups

    groups:             column of the data which represents  individual groups for Testing.
                        AB Testing will be applied for each Groups which are unique value of groups column in data.

    date:               This parameter represent calculating dates.
                        If there is time schedule for AB Test, according to date column condition will be
                        "< = date". All data before the given date (given date is included)
                        must be collected for test results.

    feature:            Represents testing values of Test.
                        Test calculation will be applied according the feature column

    data_source:        AWS RedShift, BigQuery, PostgreSQL, csv, json files can be connected to system
                        E.g.
                        {"data_source": ..., "db": ..., "password": ..., "port": ..., "server": ..., "user": ...}

    data_query_path:    if there is file for data importing;
                            must be the path (e.g /../.../ab_test_raw_data.csv)
                        if there is ac- connection such as PostgreSQL / BigQuery
                            query must at the format "SELECT++++*+++FROM++ab_test_table_+++"

    time_indicator      This can only be applied with date. It can be hour, day, week, week_part, quarter, year, month.
                        Individually time indicator checks the date part is significantly
                        a individual group for data set or not.
                        If it is uses time_indicator as a  group

    export_path        exporting the results data to. only path is enough for importing data with .csv format.
    """
    def __init__(self,
                 test_groups, groups=None, date=None, feature=None, data_source=None,
                 data_query_path=None, time_period=None, time_indicator=None, time_schedule=None,
                 export_path=None, connector=None):
        self.test_groups = test_groups
        self.groups = groups
        self.date = date
        self.feature = feature
        self.data_source = data_source
        self.data_query_path = data_query_path
        self.time_period = time_period
        self.time_indicator = time_indicator
        self.time_schedule = time_schedule
        self.export_path = export_path
        self.connector = connector
        self.arguments = {"test_groups": test_groups,
                          "groups": groups,
                          "date": date,
                          "feature": feature,
                          "data_source": data_source,
                          "data_query_path": data_query_path,
                          "time_period": time_period,
                          "time_indicator": time_indicator, "export_path": export_path}
        self.arg_terminal = {"test_groups": "TG",
                             "groups": "G",
                             "date": "D",
                             "feature": "F",
                             "data_source":  "DS",
                             "data_query_path": "DQP",
                             "time_period": "TP",
                             "time_indicator": "TI" ,"export_path": "EP"}
        self.args_str = ""
        self.ab_test = None
        self.path = get_folder_path()

    def arguments_for_subprocess(self):
        """
        when scheduling the AB Test, it is running from shell format EX: "python .../../../scheduler_service.py
        these arguments combined with string format to call with subprocess library.
        """
        for arg in self.arg_terminal:
            self.args_str += "-" + self.arg_terminal[arg] + " " + self.arguments[arg]

    def connector(self):
        """
        İf data
        """
        config = conf('config')
        try:
            if self.data_source not in ["csv", "json"]:
                for i in config['db_connection']:
                    config['db_connection'][i] = self.connector[i]

            write_yaml(join(self.path, "docs"), "configs.yaml", config, ignoring_aliases=False)
            source = GetData(data_source=self.data_source,
                             date=self.date,
                             data_query_path=self.data_query_path,
                             time_indicator=self.time_indicator,
                             feature=self.feature)
            source.get_connection()
            return True
        except Exception as e:
            print(e)
            if self.data_source not in ["csv", "json"]:
                for i in config['db_connection']:
                    config['db_connection'][i] = None
            return False

    def query_string_change(self):
        if self.data_source in ['mysql', 'postgresql', 'awsredshift', 'googlebigquery']:
            self.data_query_path = self.data_query_path.replace("\r", " ").replace("\n", " ").replace(" ", "+")

    def ab_test_init(self):
        self.query_string_change()
        if self.connector():
            self.ab_test = main(**self.arguments)
        else:
            print("pls check for data source connection / path / query.")

    def schedule_test(self):
        if self.connector():
            cmd = "python " + self.path + " " + self.args_str
            result = subprocess.Popen(cmd, shell=True)
        else:
            print("pls check for data source connection / path / query.")

    def show_dashboard(self):
        """
        if you are running dashboard make sure you have assigned export_path.
        """

