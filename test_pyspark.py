import pytest

import csv
import datetime as dt
import re


def match_house_number(hn_record, segment):
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


def countyname2borocode(county_name):
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


def street_segmentid_lookup(v_record, physicalID_list):
    for segment in physicalID_list:
        street = v_record['Street Name'].lower()
        # print(type(int(segment['BOROCODE'])), type(v_record['Violation County']))
        # first check county code and street name
        if (v_record['Violation County'] == int(segment['BOROCODE'])) and \
           ((street == segment['FULL_STREE'].lower()) or (street == segment['ST_LABEL'].lower())):
           # then, check house number: odd number is stored in left
           if match_house_number(v_record['House Number'], segment):
               return segment['PHYSICALID']
    # returns -1 if there is no match
    return -1


# initialize pyspark
from pyspark import SparkContext
sc = SparkContext()


class TestPySpark:

    def test_preprocess(self):
        """ Test the process of input data to assigning segment IDs. """
        # load lookup table
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
            assert len(filtered_records) == 76
            # convert county name to borogh code
            borocode_converted = []
            for row in filtered_records:
                row['Violation County'] = countyname2borocode(row['Violation County'])
                borocode_converted.append(row)
            assert len(borocode_converted) == 76
        # filter out samples with unknonwn county name
        borocode_converted = list(filter(lambda x: x['Violation County'] > 0, borocode_converted))
        assert len(borocode_converted) == 63
        # assign street segment id to each sample
        id_assigned = []
        for row in borocode_converted:
            id = street_segmentid_lookup(row, lookup)
            row.update({"PHYSICALID":id})
            id_assigned.append(row)
        assert len(id_assigned) == 63
        # filter out samples with unknonwn street segment
        id_assigned = list(filter(lambda x: int(x['PHYSICALID']) > 0, id_assigned))
        assert len(id_assigned) == 49

    def test_mapping(self):
        """
        Test mapping preprocessed data by segment id and fiscal year using
        dummy data.
        """
        file = 'segmentid_fiscalyear_dummy.csv'
        # to skip header
        data = sc.textFile(file)
        header = data.first()
        # start computation
        res = sc.textFile(file) \
                .filter(lambda x: x != header) \
                .mapPartitions(lambda x: csv.reader(x)) \
                .map(lambda x: ((x[0], dt.datetime.strptime(x[1], '%m/%d/%Y').year), 1)) \
                .reduceByKey(lambda x, y: x + y) \
                .sortByKey(True, 1) \
                .collect()
        expected = [(('1000', 2018), 2),
                     (('1000', 2019), 3),
                     (('2000', 2016), 1),
                     (('2000', 2017), 2),
                     (('3000', 2016), 1),
                     (('3000', 2017), 2),
                     (('3000', 2018), 3),
                     (('4000', 2017), 3),
                     (('4000', 2018), 2),
                     (('4000', 2019), 1)]
        for r, e in zip(res, expected):
            assert r == e

    def ols(self, data):
        """ data = [(x1, y1), ..., (xi, yi), ..., (xN, yN)] """
        x_bar = sum([d[0] for d in data])/len(data)
        y_bar = sum([d[1] for d in data])/len(data)
        numerator = sum([(d[0] - x_bar)*(d[1] - y_bar) for d in data])
        denomenator = sum([(d[0] - x_bar)**2 for d in data])
        return numerator/denomenator

    def test_ols(self):
        """ Test ols function """
        data = [(2015, 100), (2016, 200), (2017, 300), (2018, 400), (2019, 500)]
        assert self.ols(data) == 100
        data = [(2015, 500), (2016, 400), (2017, 300), (2018, 200), (2019, 100)]
        assert self.ols(data) == -100

    def test_ols_computation(self):
        """ Test computing ols coefficient for each street segment """
        file = 'segmentid_fiscalyear_dummy.csv'
        # to skip header
        data = sc.textFile(file)
        header = data.first()
        # return coefficient
        res = sc.textFile(file) \
                .filter(lambda x: x != header) \
                .mapPartitions(lambda x: csv.reader(x)) \
                .map(lambda x: ((x[0], dt.datetime.strptime(x[1], '%m/%d/%Y').year), 1)) \
                .reduceByKey(lambda x, y: x + y) \
                .sortByKey(True, 1) \
                .map(lambda x: (x[0][0], [(x[0][1], x[1])])) \
                .reduceByKey(lambda x, y: x + y) \
                .mapValues(lambda x: self.ols(x)) \
                .collect()
        expected = [('1000', 1.0), ('2000', 1.0), ('3000', 1.0), ('4000', -1.0)]
        for r, e in zip(res, expected):
            assert r == e

    # def test_output
