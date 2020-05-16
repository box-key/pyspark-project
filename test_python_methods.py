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

    def test_mapper_between_county_name_and_borocode(self):
        """
        Test mapper between county name in violation records and nyc_cscl file.
        """
        def countyname2borocode(county_name):
            if (county_name == 'NYC') or (county_name == 'NY'):
                return 1
            elif (county_name == 'BRONX') or (county_name == 'BX'):
                return 2
            elif (county_name == 'KINGS') or (county_name == 'K'):
                return 3
            elif (county_name == 'QUEEN') or (county_name == 'Q'):
                return 4
            elif (county_name == 'RICH') or (county_name == 'RC'):
                return 5
            else:
                return -1


        assert countyname2borocode('NYC') == 1
        assert countyname2borocode('NY') == 1
        assert countyname2borocode('BX') == 2
        assert countyname2borocode('BRONX') == 2
        assert countyname2borocode('K') == 3
        assert countyname2borocode('KINGS') == 3
        assert countyname2borocode('QUEEN') == 4
        assert countyname2borocode('Q') == 4
        assert countyname2borocode('RICH') == 5
        assert countyname2borocode('RC') == 5
        assert countyname2borocode('103') == -1
        assert countyname2borocode('nan') == -1
        assert countyname2borocode(None) == -1


    def test_assign_street_segmetn(self):
        """
        Tets a method that assigns stree segment ID in nyc_cscl.csv to
        records in vioaltion file.
        """
        # load violation records
        with open('violation_small.csv', 'r') as f:
            file = csv.DictReader(f)
            filtered_records = \
                list(filter(lambda x: (2015 <= dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year) and \
                                      (dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year <= 2019),
                            file))
        # load nyc_cscl
        with open('data\\nyc_cscl.csv', 'r') as f:
            file = csv.DictReader(f)
            street_segments = [row for row in file]
        assert len(street_segments) == 547313
