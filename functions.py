from itertools import product, combinations
import numpy as np
import pandas as pd
import random
from math import sqrt
from scipy import stats

from data_access import GetData
from utils import *
from configs import time_dimensions, day_of_year, time_indicator_accept_threshold, s_size_ratio


def data_manipulation(date, time_indicator, feature, data_source, groups, data_query_path):
    data_process = GetData(data_source=data_source,
                           data_query_path=data_query_path,
                           time_indicator=time_indicator,
                           feature=feature,
                           date=date)
    data_process.data_execute()
    print("data size :", len(data_process.data))
    print("remove null values ..")
    data_process.data = data_process.data[data_process.data[feature] == data_process.data[feature]]
    print("check for time part data ..")
    data, groups = data_process.data, split_groups(groups)
    if time_indicator is not None:
        date_features = TimePartFeatures(job='train',
                                         data=data_process.data,
                                         time_indicator=time_indicator,
                                         groups=groups,
                                         feature=feature)
        date_features.decide_timepart_of_group()
        data, groups = date_features.data, date_features.groups
    return data, groups


class TimePartFeatures:
    def __init__(self, job=None, data=None, time_indicator=None, groups=None, feature=None):
        self.job = job
        self.data = data
        self._data = data
        self.time_indicator = time_indicator
        self.groups = groups
        self.time_groups = None
        self.date = time_indicator
        self.feature = feature
        self.time_dimensions, self.smallest_time_indicator = smallest_time_part(list(self.data[self.time_indicator]))
        self.time_dimensions_accept = {d: False for d in self.time_dimensions}
        self.threshold = time_indicator_accept_threshold['threshold']
        self.accept_ratio_value = time_indicator_accept_threshold['accept_ratio_value']

    def remove_similar_time_dimensions(self, part):
        accept = False
        if part == 'year':
            if self.smallest_time_indicator not in ['week', 'week_part', 'week_day', 'day']:
                accept = True
        if part == 'quarter':
            if self.smallest_time_indicator not in ['week', 'week_part', 'week_day', 'day']:
                if not self.time_dimensions_accept['year']:
                    accept = True
        if part == 'month':
            if self.smallest_time_indicator not in ['week', 'week_part', 'week_day', 'day']:
                if not self.time_dimensions_accept['quarter']:
                    accept = True
        if part == 'week':
            if self.smallest_time_indicator in ['hour', 'min', 'second']:
                if len([1 for p in ['year', 'quarter', 'month'] if self.time_dimensions_accept[p]]) == 0:
                    accept = True
        if part == 'week_part':
            if self.smallest_time_indicator in ['day', 'hour']:
                accept = True
        if part == 'week_day':
            if self.smallest_time_indicator in ['hour', 'min', 'second']:
                if not self.time_dimensions_accept['week_part']:
                    accept = True
        if part == 'day_part':
            if self.smallest_time_indicator in ['min', 'second']:
                accept = True
        if part == 'hour':
            if self.smallest_time_indicator == 'second':
                if not self.time_dimensions_accept['day_part']:
                    accept = True
        return accept

    def iteration_count(self, s1, s2):
        iter = int(min(len(s1), len(s2)) * 0.0001)
        if 1000 < min(len(s1), len(s2)) < 10000:
            iter = int(min(len(s1), len(s2)) * 0.01)
        if min(len(s1), len(s2)) < 1000:
            iter = int(min(len(s1), len(s2)) * 0.1)
        return iter

    def get_threshold(self, part):
        update_values = False
        if part == 'quarter':
            if self.time_dimensions_accept['year']:
                update_values = True
        if part == 'month':
            if self.time_dimensions_accept['year'] or self.time_dimensions_accept['quarter']:
                update_values = True
        if part == 'week':
            if len([1 for p in ['year', 'quarter', 'month'] if self.time_dimensions_accept[p]]) != 0:
                update_values = True

        if update_values:
            self.threshold = time_indicator_accept_threshold['threshold'] - 0.2
            self.accept_ratio_value = time_indicator_accept_threshold['accept_ratio_value'] + 0.2

    def time_dimension_decision(self, part):
        if self.remove_similar_time_dimensions(part):
            self.get_threshold(part=part)
            accept_count = 0
            combs = list(combinations(list(self.data[part].unique()), 2))
            for comb in combs:
                sample_1 = sampling(list(self._data[self._data[part] == comb[0]][self.feature]), sample_size=100000)
                sample_2 = sampling(list(self._data[self._data[part] == comb[1]][self.feature]), sample_size=100000)
                iter = self.iteration_count(sample_1, sample_2)
                h0_accept_ratio, params = boostraping_calculation(sample1=sample_1,
                                                                  sample2=sample_2,
                                                                  iteration=iter,
                                                                  sample_size=int(
                                                                      min(len(sample_1), len(sample_2)) * s_size_ratio),
                                                                  alpha=0.05)
                accept_count += 1 if h0_accept_ratio < self.threshold else 0
            accept_ratio = len(combs) * self.accept_ratio_value  # 50%
            print("Time Part :", part, "Accept Treshold :", accept_ratio, "Accepted Count :", accept_count)
            return True if accept_count > accept_ratio else False

    def day_decision(self):
        return True if self.smallest_time_indicator in ['min', 'second'] else False

    def year_decision(self):
        return True if int(self.time_diff / 60 / 60 / 24) >= (day_of_year * 2) else False

    def quarter_decision(self):
        return True if int(self.time_diff / 60 / 60 / 24) >= (day_of_year * 1) else False

    def check_for_time_difference_ranges_for_accepting_time_part(self, part):
        decision = False
        if part == 'year':
            decision = self.year_decision()
        if part == 'quarter':
            decision = self.quarter_decision()
        if part == 'week_day':
            decision = self.day_decision()
        return decision

    def decide_timepart_of_group(self, part):
        print("*" * 5, "decision for time part :", part, "*" * 5)
        result = False
        (unique, counts) = np.unique(list(self.data[part]), return_counts=True)
        if len(unique) >= 2:
            if 1 not in counts:
                if part not in ['week_day', 'hour', 'min', 'second']:
                    if self.check_for_time_difference_ranges_for_accepting_time_part(part):
                        result = self.time_dimension_decision(part)
                else:
                    if part == 'week_day':
                        results = self.check_for_time_difference_ranges_for_accepting_time_part(part)
                    if self.smallest_time_indicator == 'second' and part == 'hour':
                        result = self.time_dimension_decision(part)
        print("result :", " INCLUDING" if result else "EXCLUDING")
        self.time_dimensions_accept[part] = result
        return result

    def calculate_date_parts(self):
        accepted_date_parts = []
        for t_dimension in self.time_dimensions:
            if t_dimension not in self.groups:
                self.data[t_dimension] = self.data[self.date].apply(lambda x: date_part(x, t_dimension))
                if self.decide_timepart_of_group(part=t_dimension):
                    accepted_date_parts.append(t_dimension)
        self.time_groups = accepted_date_parts
        self.groups += accepted_date_parts

    def date_dimension_deciding(self):
        if self.time_indicator is not None:
            self.time_diff = get_time_difference(list(self.data[self.time_indicator]))
            self.calculate_date_parts()


def smallest_time_part(dates):
    sample_dates = random.sample(dates, int(len(dates) * 0.8))
    smallest = False
    t_dimensions = list(reversed(time_dimensions))
    count = 0
    if len(np.unique(sample_dates)) != 1:
        while not smallest:
            (unique, counts) = np.unique([date_part(d, t_dimensions[count]) for d in sample_dates], return_counts=True)
            if len(unique) >= 2:
                smallest = True
                smallest_td = t_dimensions[count]
                break
            count += 1
        accepted_t_dimensions = list(reversed(t_dimensions[count + 1:]))
    else:
        accepted_t_dimensions, smallest_td = t_dimensions, t_dimensions[0]
    return accepted_t_dimensions, smallest_td  # smallest time indicator not included to time_dimensions


def get_time_difference(dates):
    return (max(dates) - min(dates)).total_seconds()


def sampling(sample, sample_size):
    if len(sample) <= sample_size:
        return sample
    else:
        return random.sample(sample, sample_size)


def get_sample_size(ratio, sample):
    return int(ratio * len(sample))


def boostraping_calculation(sample1, sample2, iteration, sample_size, alpha):
    """
    Randomly selecting samples from two population on each iteration.
    Iteratively independent test are applied each randomly selected samples
    :param sample1: list of values related to sample 1
    :param sample2: list of values related to sample 2
    :param iteration: numeber of iteration. It is better to asssign higher values.
    :param sample_size: number of sample when randomly sampled from each data set.
                        Make sure this parameters bigger than both sample of size
    :param alpha: Confidence level
    :param two_sample: Is it related to Two Sample Test or not.
    :return: HO_accept ratio: num of accepted testes / iteration
             result data set: each iteration of test outputs of pandas data frame
    """
    pval_list, h0_accept_count, test_parameters_list= [], 0, []

    for i in range(iteration):
        d = {'sample_ratio': sample_size, 'confidence_level': alpha}
        d['size1'], d['size2'] = get_sample_size(d['sample_ratio'], sample1), get_sample_size(d['sample_ratio'], sample1)
        random1 = sampling(sample=sample1,
                           sample_size=d['size1'])  # random.sample(sample1, sample_size)  # randomly picking samples from sample 1
        random2 = sampling(sample=sample2,
                           sample_size=d['size2'])  # random.sample(sample2, sample_size)  # randomly picking samples from sample 2
        d['mean1'], d['mean2'] = np.mean(random1), np.mean(random2)
        d['var1'], d['var2'] = np.var(random1), np.var(random2)
        d['pval'], d['confidence_intervals'], hypotheses_accept = calculate_t_test(d['mean1'], d['mean2'],
                                                                                   d['var1'], d['var2'],
                                                                                   d['size1'], d['size2'],
                                                                                   d['confidence_level'])
        d['h0_accept'] = 1 if hypotheses_accept == 'HO ACCEPTED!' else 0
        test_parameters_list.append(d)
    return test_parameters_list


def calculate_t_test(mean1, mean2, var1, var2, n1, n2, alpha):
    """
    It Test according to T- Distribution of calculations
    There are two main Hypotheses Test are able to run. Two Sample Two Tail Student-T Test, One Sample Student-T-Test
    In order to test two main parameters are needed Mean and Variance, T~(M1, Var)
    :param mean1: Mean of Sample 1
    :param mean2: Mean of Sample 2. If one sample T-Test assign None
    :param var1: Variance of Sample 1.
    :param var2: Variance of Sample 2. If one sample T-Test assign None
    :param n1: sample 1 size
    :param n2: sample 2 size. If one sample T-Test assign None
    :param alpha: Confidence level
    :param two_sample: Boolean; True - False
    :return: returns p-value of test, confidence interval of test, H0 Accepted!! or H0 REjected!!
    """
    # Two Sample T Test (M0 == M1) (Two Tails)
    t = (mean1 - mean2) / sqrt((var1 / n1) + (var2 / n2))  # t statistic calculation for two sample
    df = n1 + n2 - 2  # degree of freedom for two sample t - set
    pval = 1 - stats.t.sf(np.abs(t), df) * 2  # two-sided pvalue = Prob(abs(t)>tt) # p - value
    cv = stats.t.ppf(1 - (alpha / 2), df)
    standart_error = cv * sqrt((var1 / n1) + (var2 / n2))
    confidence_intervals = [abs(mean1 - mean2) - standart_error, abs(mean1 - mean2) + standart_error, standart_error]
    acception = 'HO REJECTED!' if pval < (alpha / 2) else 'HO ACCEPTED!'  # left tail
    acception = 'HO REJECTED!' if pval > 1 - (alpha / 2) else 'HO ACCEPTED!'  # right tail
    return pval, confidence_intervals, acception


def get_levels(data, groups):
    groups = [g for g in groups if g not in [None, '', 'none', 'null', 'None']]
    return list(product(*[list(data[data[g] == data[g]][g].unique()) for g in groups]))