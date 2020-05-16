import pytest

import pandas as pd

import csv
import datetime as dt


class TestMethods:

    def test_year_filter(self):
        """
        Test the first step of program:
        filter out records that didn't happen between 2015 and 2019.
        """
        # load data
        df = pd.read_csv('violation_small.csv')
        # filter out records not between 2015 and 2019
        df_filtered = df[df['Issue Date'].apply(
                        lambda x: (2015 <= dt.datetime.strptime(x, '%m/%d/%Y').year) and \
                                  (dt.datetime.strptime(x, '%m/%d/%Y').year <= 2019))]
        # does the same thing with csv module
        filtered_samples = []
        with open('violation_small.csv', 'r') as f:
            file = csv.DictReader(f)
            for row in file:
                date = dt.datetime.strptime(row['Issue Date'], '%m/%d/%Y')
                if (2015 <= date.year) and (date.year <= 2019):
                    filtered_samples.append(row)
        assert len(filtered_samples) == df_filtered.shape[0]
        # test filter method
        with open('violation_small.csv', 'r') as f:
            file = csv.DictReader(f)
            filtered = list(filter(lambda x: (2015 <= dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year) and \
                                             (dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year <= 2019),
                                             file))
        assert len(filtered) == len(filtered_samples) == df_filtered.shape[0]
