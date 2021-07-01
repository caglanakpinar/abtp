import threading
from multiprocessing import cpu_count
from os.path import exists, join
import shutil
from numpy import arange
from pandas import DataFrame, concat, read_csv

try:
    from functions import *
    from utils import split_test_groups, convert_str_to_day
    from configs import hyper_conf, descriptive_columns
except Exception as e:
    from .functions import *
    from .utils import split_test_groups, convert_str_to_day, execute_parallel_run
    from .configs import hyper_conf, descriptive_columns


def get_confidence_intervals(intervals):
    return np.arange(intervals[0], intervals[1], intervals[2])


def get_comb_params(distributions):
    comb_arrays = {}
    for d in distributions:
        _params = distributions[d]
        _keys = list(distributions[d].keys())
        arrays = []
        for p in _params:
            if "*" in list(_params[p]):
                arrays.append(
                              arange(float(_params[p].split("*")[0]),
                                     float(_params[p].split("*")[1]),
                                     float(_params[p].split("*")[0])).tolist()
                    )
            else:
                arrays.append([float(i) for i in _params[p].split("_")])
        comb_arrays[d] = list(product(*arrays))
    return comb_arrays


def is_numeric_value_an_integer(value):
    if str(float(value)).split(".")[-1] == '0':
        return int
    else:
        return float


def get_params(keys, comb, data):
    count, params = 0, {}
    for p in keys:
        _p = is_numeric_value_an_integer(comb[count])(comb[count])
        params[p] = _p
        count += 1
    if len(data) < 300:
        params['iteration'] = 1
        params['sample_size'] = 1
    return params


def rename_descriptives():
    d = {}
    for i in descriptive_columns:
        d[i] = i[:-1] + '_control' if i[-1] == '1' else i[:-1] + '_validation'
    return d


def assign_groups_to_results(data, groups, comb):
    if len(groups) != 0:
        count = 0
        for g in groups:
            data[g] = comb[count]
            count += 1
    return data


class Test:
    def __init__(self,
                 test_groups,
                 data=None,
                 groups=None,
                 date=None,
                 feature=None,
                 data_source=None,
                 data_query_path=None,
                 time_period=None,
                 time_indicator=None,
                 export_path=None,
                 parameters=None):
        self.date = convert_date(date)
        self.start_date = None
        self.time_indicator = time_indicator
        self.data, self.groups = data_manipulation(data_raw=data,
                                                   date=date,
                                                   time_indicator=time_indicator,
                                                   feature=feature,
                                                   data_source=data_source,
                                                   groups=groups,
                                                   data_query_path=data_query_path,
                                                   time_period=time_period)
        self.test_groups_field = test_groups
        self.test_groups_indicator = split_test_groups(self.test_groups_field, self.data)
        self.feature = feature
        self.time_period = time_period
        self.time_indicator_values = None
        self.tp = None
        self.levels = get_levels(self.data, self.groups)
        self._c, self._a = None, None
        self.f_w_data = None
        self.data_distribution = 'normal'  # by default it is Normal distribution
        self.parameter_combinations = get_comb_params(hyper_conf('distribution_parameters') if parameters is None else parameters)
        self.export_path = export_path
        self.comb, self.param_comb, self._params = None, None, {}
        self.results = []
        self.final_results = DataFrame()
        self.h0_accept_ratio = 0
        self.h0_acceptance = 0
        self.temp_folder = join(abspath(""), "temp_ab_test_results", "") if export_path is None else join(export_path,
                                                                                                          "temp_ab_test_results",
                                                                                                          "")

    def get_query(self, combination):
        count = 0
        query = ''
        for c in combination:
            if type(c) != str:
                query += self.groups[count] + ' == ' + str(c) + ' and '
            else:
                query += self.groups[count] + " == '" + str(c) + "' and "
            count += 1

        if self.date is not None and self.time_indicator is not None:
            query += self.time_indicator + " <= '" + str(self.date) + "' and "
            if self.time_period is not None:
                self.start_date = get_start_date_of_test(self.date, self.time_period)
                query += self.time_indicator + " >= '" + str(self.start_date) + "' and "
                print("query_date :", self.start_date, " - ", self.date)
        query = query[:-4]
        return query

    def get_control_and_active_data(self, f_w_data):
        _c = f_w_data[f_w_data[self.test_groups_field] == self.test_groups_indicator]
        _a = f_w_data[f_w_data[self.test_groups_field] != self.test_groups_indicator]
        return _c, _a

    def decide_distribution(self):
        _unique = list(self.data[self.feature].unique())
        _type = type(_unique[0])
        # by default it is Normal distribution
        if _type != str:
            _min, _max = min(self.data[self.feature]), max(self.data[self.feature])
            if 0 <= _min < 1 and 0 < _max <= 1:
                self.data_distribution = 'beta'
        if len(_unique) == 2:
            self.data_distribution = 'binominal'
        if 2 < len(_unique) < 30:
            if _type == int:
                if min(self.data[self.feature]) >= 0:
                    self.data_distribution = 'poisson'
            if _type == str:
                self.data_distribution = 'poisson'
        print("Distribution :", self.data_distribution)

    def get_descriptives(self, results):
        return results.rename(columns=rename_descriptives())

    def test_decision(self, _results, combination):

        _name = "_".join([c for c in combination]) if len(combination) > 1 else combination[0]
        _file = join(self.temp_folder, _name.replace(" ", "_") + ".csv")
        if self.is_boostraping_calculation():
            _results = self.get_descriptives(_results)
            h0_accept_ratio = sum(_results['h0_accept']) / len(_results)
            h0_acceptance = True if self.h0_accept_ratio > 0.5 else False
            _results['date'] = self.date
            _results['test_result'] = h0_acceptance
            _results['accept_Ratio'] = h0_accept_ratio
            _results = assign_groups_to_results(_results, self.groups, combination)

            try:
                _results = concat([read_csv(_file, index=False), _results])
                _results.to_csv(_file, index=False)
            except Exception as e:
                _results.to_csv(_file, index=False)
        else:
            _results.to_csv(_file, index=False)

    def is_boostraping_calculation(self):
        if self.date is None or self.time_indicator is None:
            return True
        else:
            if self.export_path is None:
                return True
            else:
                files = check_result_data_exits(self.export_path)
                if len(files) >= 1:
                    return True
                else:
                    return False

    def test_execute(self, _c, _a):
        results = []
        for param_comb in self.parameter_combinations[self.data_distribution]:
            _params = get_params(list(hyper_conf('normal').keys()), param_comb, self.data)
            if self.is_boostraping_calculation():
                results += boostraping_calculation(sample1=list(_c[self.feature]),
                                                   sample2=list(_a[self.feature]),
                                                   iteration=_params['iteration'],
                                                   sample_size=_params['sample_size'],
                                                   alpha=1-_params['confidence_level'],
                                                   dist=self.data_distribution)
            else:
                results += bayesian_approach(sample1=list(_c[self.feature]),
                                             sample2=list(_a[self.feature]), dist=self.data_distribution)
        return DataFrame(results)

    def check_for_time_period(self):
        return True if self.time_period is None else False

    def run_test(self, combination):
        try:
            f_w_data = self.data.query(self.get_query(combination))
            if len(f_w_data) != 0:
                _c, _a = self.get_control_and_active_data(f_w_data)
                _results = self.test_execute(_c, _a)
                self.test_decision(_results, combination)
        except Exception as e:
            print("running test results error :", e)

    def execute(self):
        try:
            os.mkdir(self.temp_folder)
        except Exception as e:
            print("recreating 'temp_results' folder ...")
            shutil.rmtree(self.temp_folder)
            os.mkdir(self.temp_folder)

        print("time period :", self.time_period)
        self.decide_distribution()
        iters = int(len(self.levels) / 1024) + 1
        for i in range(iters):
            print("main iteration :", str(i), " / ", str(iters))
            _sample_levels = get_iter_sample(self.levels, i, iters, 1024)
            execute_parallel_run(_sample_levels, self.run_test, arguments=None, parallel=8)

        for comb in listdir(dirname(self.temp_folder)):
            try:
                self.final_results = concat([self.final_results, read_csv(join(self.temp_folder, comb))])
            except Exception as e:
                pass

        try:
            shutil.rmtree(self.temp_folder)
        except Exception as e:
            pass










