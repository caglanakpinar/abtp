import threading
from multiprocessing import cpu_count
from numpy import arange
from pandas import DataFrame

from functions import *
from utils import split_test_groups
from configs import descriptive_columns, hyper_conf


def get_confidence_intervals(intervals):
    return np.arange(intervals[0], intervals[1], intervals[2])


def get_comb_params(distributions):
    comb_arrays = {}
    for d in distributions:
        _params = distributions[d]
        _keys = list(distributions[d].keys())
        arrays = []
        for p in _params:
            arrays.append(
                          arange(float(_params[p].split("*")[0]),
                                 float(_params[p].split("*")[1]),
                                 float(_params[p].split("*")[0])).tolist()
                )
        comb_arrays[d] = list(product(*arrays))
    return comb_arrays


def is_numeric_value_an_integer(value):
    if str(float(value)).split(".")[-1] == '0':
        return int
    else:
        return float


def get_params(keys, comb):
    count, params = 0, {}
    for p in keys:
        asd = is_numeric_value_an_integer(comb[count])
        print()
        _p = is_numeric_value_an_integer(comb[count])(comb[count])
        params[p] = _p
        count += 1
    print(params)
    return params


def rename_descriptives():
    d = {}
    for i in descriptive_columns:
        d[i] = i[:-1] + '_control' if i[-1] == '1' else  i[:-1] + '_validation'
    return d


class ABTest:
    def __init__(self, test_groups, groups=None, date=None, feature=None,
                 data_source=None, data_query_path=None, time_period=None, time_indicator=None):
        self.date = date
        self.time_indicator = time_indicator
        self.data, self.groups = data_manipulation(date=self.date,
                                                   time_indicator=time_indicator,
                                                   feature=feature,
                                                   data_source=data_source,
                                                   groups=groups,
                                                   data_query_path=data_query_path)
        self.test_groups_field = test_groups
        self.test_groups_indicator = split_test_groups(self.test_groups_field, self.data)
        self.date = date
        self.feature = feature
        self.time_period = time_period
        self.levels = get_levels(self.data, self.groups)
        self._c, self._a = None, None
        self.f_w_data = None
        self.data_distribution = 'normal'  # by default it is Normal distribution
        self.parameter_combinations = get_comb_params(hyper_conf('distribution_parameters'))
        self.comb, self.param_comb, self._params = None, None, {}
        self.results = []
        self.final_results = DataFrame()
        self.h0_accept_ratio = 0
        self.acceptance = 0

    def get_query(self):
        count = 0
        query = ''
        for c in self.comb:
            if type(c) != str:
                query += self.groups[count] + ' == ' + str(c) + ' and '
            else:
                query += self.groups[count] + " == '" + str(c) + "' and "
            count += 1
        query = query[:-4]
        return query

    def get_control_and_active_data(self):
        self._c = self.f_w_data[self.f_w_data[self.test_groups_field] == self.test_groups_indicator]
        self._a = self.f_w_data[self.f_w_data[self.test_groups_field] != self.test_groups_indicator]

    def decide_test_values(self):
        _unique = list(self.data[self.feature].unique())
        _type = type(_unique[0])
        _min, _max = min(self.data[self.feature]), max(self.data[self.feature])
        # by default it is Normal distribution
        if 0 <= _min <= 1 and 0 <= _max <= 1:
            self.data_distribution = 'beta'
        if len(_unique) == 2:
            self.data_distribution = 'binominal'
        if len(_unique) < 30:
            if _type in [str, int]:
                self.data_distribution = 'poisson'
        if len(_unique) > 30:
            if _type == str:
                self.data_distribution = 'gamma'

    def test_execute(self):
        if self.data_distribution == 'normal':
            self.normal_dist_test()
        if self.data_distribution == 'beta':
            self.beta_dist_test()
        if self.data_distribution == 'binominal':
            self.binominal_dist_test()
        if self.data_distribution == 'poisson':
            self.poisson_dist_test()
        if self.data_distribution == 'gamma':
            self.gamma_dist_test()

    def get_descriptives(self):
        self.final_results = self.results[descriptive_columns]
        self.final_results = self.final_results.rename(columns=rename_descriptives())
        print()

    def test_decision(self):
        self.get_descriptives()
        self.h0_accept_ratio = sum(self.results['h0_accept']) / len(self.results)
        if self.h0_accept_ratio > 0.5:
            self.acceptance = True
        self.final_results['date'], self.final_results['test_result'], self.final_results['accept_Ratio'] = self.date, self.acceptance, self.h0_accept_ratio
        print()

    def normal_dist_test(self):
        self.results = []
        for self.param_comb in self.parameter_combinations[self.data_distribution]:
            self._params = get_params(list(hyper_conf('normal').keys()), self.param_comb)
            print(self._params)
            t1 = datetime.datetime.now()
            self.results += boostraping_calculation(sample1=list(self._c[self.feature]),
                                                    sample2=list(self._a[self.feature]),
                                                    iteration=self._params['iteration'],
                                                    sample_size=self._params['sample_size'],
                                                    alpha=1-self._params['confidence_level'])
            print(abs((t1 - datetime.datetime.now()).total_seconds()) / 60)
        self.results = DataFrame(self.results)

    def execute(self):
        self.decide_test_values()
        for self.comb in self.levels:
            print("*" * 4, "AB TEST - ", self.get_query().replace(" and ", "; ").replace(" == ", " - "), "*" * 4)
            self.f_w_data = self.data.query(self.get_query())
            print("data size :", len(self.f_w_data))
            self.get_control_and_active_data()
            self.decide_test_values()
            self.test_execute()
            self.test_decision()


