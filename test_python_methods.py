import pytest

import pandas as pd

import csv
import datetime as dt
import re


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

    def countyname2borocode(self, county_name):
        if (county_name == 'NEW Y') or (county_name == 'NEWY') or (county_name == 'NY') or (county_name == 'MH') or (county_name == 'MAN'):
            return 1
        elif (county_name == 'BRONX') or (county_name == 'BX'):
            return 2
        elif (county_name == 'KINGS') or (county_name == 'KING') or (county_name == 'K'):
            return 3
        elif (county_name == 'QUEEN') or (county_name == 'QU') or (county_name == 'Q'):
            return 4
        elif (county_name == 'R'):
            return 5
        else:
            return -1

    def test_mapper_between_county_name_and_borocode(self):
        """
        Test mapper between county name in violation records and nyc_cscl file.
        """
        assert self.countyname2borocode('NEW Y') == 1
        assert self.countyname2borocode('NEWY') == 1
        assert self.countyname2borocode('NY') == 1
        assert self.countyname2borocode('MH') == 1
        assert self.countyname2borocode('MAN') == 1
        assert self.countyname2borocode('BX') == 2
        assert self.countyname2borocode('BRONX') == 2
        assert self.countyname2borocode('K') == 3
        assert self.countyname2borocode('KING') == 3
        assert self.countyname2borocode('KINGS') == 3
        assert self.countyname2borocode('QUEEN') == 4
        assert self.countyname2borocode('QU') == 4
        assert self.countyname2borocode('Q') == 4
        assert self.countyname2borocode('R') == 5
        assert self.countyname2borocode('nan') == -1
        assert self.countyname2borocode(None) == -1


    def test_convert_countyname_in_dataset(self):
        """
        Tets a method that assigns stree segment ID in nyc_cscl.csv to
        records in vioaltion file.
        """
        # load violation records
        with open('violation_small.csv', 'r') as f:
            file = csv.DictReader(f)
            # only keep records betwen 2015 and 2019
            filtered_records = \
                list(filter(lambda x: (2015 <= dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year) and \
                                      (dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year <= 2019),
                            file))
            # convert county name in each row to borocode
            borocode_converted = []
            for row in filtered_records:
                borocode = self.countyname2borocode(row['Violation County'])
                if borocode > 0:
                    row['Violation County'] = borocode
                    borocode_converted.append(row)
        assert len(filtered_records) == 76
        assert len(borocode_converted) == 63
        # make sure borocode is in [1, 2, 3, 4, 5]
        correct_codes = [1, 2, 3, 4, 5]
        for row in borocode_converted:
            assert row['Violation County'] in correct_codes

    def match_house_number(self, hn_record, segment):
        # exclude single character house numbers
        if (len(hn_record) == 1) and (not hn_record.isnumeric()):
            return False
        # if a record is empty, assigns 0
        if len(hn_record) == 0:
            hn_record = 0
        # otherwise concatenate two values together
        # example: '187-09' = 18709 <int>
        # example: '187' = 187 <int>
        else:
            hn_record = int(hn_record.replace('-', ''))
        # format house numbers in lookup segment in the same way
        # if hn_record is even, we should use 'R'; otherwise, 'L'
        if hn_record%2 == 0:
            if len(segment['R_LOW_HN']) == 0:
                lower = 0
            else:
                lower = int(re.sub('-0|-', '', segment['R_LOW_HN']))
            if len(segment['R_HIGH_HN']) == 0:
                high = 0
            else:
                high = int(re.sub('-0|-', '', segment['R_HIGH_HN']))
        else:
            if len(segment['L_LOW_HN']) == 0:
                lower = 0
            else:
                lower = int(re.sub('-0|-', '', segment['L_LOW_HN']))
            if len(segment['L_HIGH_HN']) == 0:
                high = 0
            else:
                high = int(re.sub('-0|-', '', segment['L_HIGH_HN']))
        return (lower <= hn_record) and (hn_record <= high)

    def test_match_house_number(self):
        """ Test match_house_number (a helper method) """
        segment = {'R_LOW_HN': '178', 'R_HIGH_HN': '200',
                   'L_LOW_HN': '1', 'L_HIGH_HN': '3'}
        # non numeric single character is always false
        assert not self.match_house_number('W', segment)
        assert not self.match_house_number('S', segment)
        assert self.match_house_number('3', segment)
        # check input without dash '-'
        assert self.match_house_number('190', segment)
        # check input with dash
        segment = {'R_LOW_HN': '178-10', 'R_HIGH_HN': '200-12',
                   'L_LOW_HN': '178-11', 'L_HIGH_HN': '200-13'}
        # check even numbers
        assert self.match_house_number('178-10', segment)
        assert self.match_house_number('200-12', segment)
        assert self.match_house_number('190-22', segment)
        assert not self.match_house_number('168-22', segment)
        assert not self.match_house_number('222-22', segment)
        # check odd numbers
        assert self.match_house_number('178-11', segment)
        assert self.match_house_number('200-13', segment)
        assert self.match_house_number('190-21', segment)
        assert not self.match_house_number('168-09', segment)
        assert not self.match_house_number('200-15', segment)
        # check empty house number
        segment = {'R_LOW_HN': '0', 'R_HIGH_HN': '0',
                   'L_LOW_HN': '178-11', 'L_HIGH_HN': '200-13'}
        assert self.match_house_number('', segment)
        segment = {'R_LOW_HN': '', 'R_HIGH_HN': '',
                   'L_LOW_HN': '178-11', 'L_HIGH_HN': '200-13'}
        assert self.match_house_number('', segment)
        # test 6 digits
        segment = {'R_LOW_HN': '178-012', 'R_HIGH_HN': '200-004',
                   'L_LOW_HN': '178-011', 'L_HIGH_HN': '200-003'}
        # odd numbers
        assert self.match_house_number('200-03', segment)
        assert self.match_house_number('178-11', segment)
        assert self.match_house_number('192-11', segment)
        assert not self.match_house_number('999-99', segment)
        assert not self.match_house_number('102-01', segment)
        # even numbers
        assert self.match_house_number('200-04', segment)
        assert self.match_house_number('178-12', segment)
        assert self.match_house_number('199-12', segment)
        assert not self.match_house_number('999-98', segment)
        assert not self.match_house_number('102-00', segment)

    def street_segmentid_lookup(self, v_record, physicalID_list):
        for segment in physicalID_list:
            street = v_record['Street Name'].lower()
            # print(type(int(segment['BOROCODE'])), type(v_record['Violation County']))
            # first check county code and street name
            if (v_record['Violation County'] == int(segment['BOROCODE'])) and \
               ((street == segment['FULL_STREE'].lower()) or (street == segment['ST_LABEL'].lower())):
               # then, check house number: odd number is stored in left
               if self.match_house_number(v_record['House Number'], segment):
                   return segment['PHYSICALID']
        # returns -1 if there is no match
        return -1

    def test_street_segmentid_lookup(self):
        """ Test street_segmentid_lookup method """
        # load physical ID list
        with open('data\\nyc_cscl.csv', 'r') as f:
            file = csv.DictReader(f)
            lookup = [row for row in file]
        # load violation records
        with open('violation_small.csv', 'r') as f:
            file = csv.DictReader(f)
            # only keep records betwen 2015 and 2019
            filtered_records = \
                list(filter(lambda x: (2015 <= dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year) and \
                                      (dt.datetime.strptime(x['Issue Date'], '%m/%d/%Y').year <= 2019),
                            file))
            # convert county name in each row to borocode
            borocode_converted = []
            for row in filtered_records:
                borocode = self.countyname2borocode(row['Violation County'])
                if borocode > 0:
                    row['Violation County'] = borocode
                    borocode_converted.append(row)
        # test match between borocode and violation county
        for ex in borocode_converted[:10]:
            found = False
            for segment in lookup:
                if int(segment['BOROCODE']) == ex['Violation County']:
                    found = True
                    break
            assert found
        # test match between street names
        for ex in borocode_converted[:10]:
            found = False
            st = ex['Street Name'].lower()
            for segment in lookup:
                if (st == segment['FULL_STREE'].lower()) or \
                   (st == segment['ST_LABEL'].lower()):
                    # print(st, segment['FULL_STREE'].lower(), segment['ST_LABEL'].lower())
                    found = True
                    break
            assert found
        # test match between house number
        for ex in borocode_converted[:10]:
            hn = ex['House Number']
            if len(hn) >= 2:
                hn = int(hn.replace('-', ''))
                assert (hn%2 == 1) or (hn%2 == 0)
        # test function
        print()
        for ex in borocode_converted:
            res = int(self.street_segmentid_lookup(ex, lookup))
            print(ex['House Number'])
            print(res)
            print()
