import pytest

from pyspark import SparkContext
from pyspark.sql import SparkSession

from collections import defaultdict

import csv
import os
import datetime as dt
import re


sc = SparkContext()

NYC_CSCL_PATH = 'nyc_cscl.csv'
root = 'data'
violation_records = [os.path.join(root, 'violation_small1.csv'),
                     os.path.join(root, 'violation_small2.csv')]
VIOLATION_PATH = ','.join(violation_records)


def construct_lookup(data):
    # data = [(PHYSICALID, L_LOW_HN, L_HIGH_HN, R_LOW_HN, R_HIGH_HN, ST_LABEL, BOROCODE_IDX, FULL_STREE)]
    lookup = defaultdict(list)
    for row in data:
        # format outputs
        id = int(row[0])
        l_low = 0 if len(row[1]) == 0 else int(re.sub('-0|-', '', row[1]))
        l_high = 0 if len(row[2]) == 0 else int(re.sub('-0|-', '', row[2]))
        r_low = 0 if len(row[3]) == 0 else int(re.sub('-0|-', '', row[3]))
        r_high = 0 if len(row[4]) == 0 else int(re.sub('-0|-', '', row[4]))
        st_label = row[5].lower()
        borocode = int(row[6])
        full_stree = row[7].lower()
        # add formatted elements to table
        lookup[(st_label, borocode)].append(((r_low, r_high), (l_low, l_high), id))
        lookup[(full_stree, borocode)].append(((r_low, r_high), (l_low, l_high), id))
    return lookup


def format_hn(hn_record):
    # if a record is empty, assigns 0
    if len(hn_record) == 0:
        return 0
    # otherwise concatenate two values together
    # example: '187-09' = 18709 <int>
    # example: '187' = 187 <int>
    else:
        # format cases like `70 23` -> `70-23`
        hn_record = re.sub('\s', '-', hn_record)
        # exclude cases like `123A`, 'W', 'S', etc.
        try:
            return int(hn_record.replace('-', ''))
        except ValueError:
            return -1


# violation_record = [year, borocode, house_number, street_name]
def lookup_street_segment(v_record, lookup_table):
    street_name = v_record[3].lower() # lower string
    house_number = v_record[2] # <str>
    borocode = v_record[1] # <int>
    # lookup table to get candidates
    hn_ranges = lookup_table[(street_name, borocode)]
    # if key doesn't exist, it returns empty list
    if len(hn_ranges) == 0:
        return -1
    # format house number, if output is -1, returns -1
    formatted_hn = format_hn(house_number)
    if formatted_hn == -1:
        return -1
    # check candidate ranges, if there is a match, returns physicalID
    for hn_range in hn_ranges:
        # hn_range = ((r_low, r_high), (l_low, l_high), physicalID)
        ran = hn_range[formatted_hn%2]
        # ran = (low, high)
        if (ran[0] <= formatted_hn) and (formatted_hn <= ran[1]):
            return hn_range[2]
    # if there is no match, returns -1
    return -1


class TestDictLookup:

    def test_construct_dict(self):
        table = sc.textFile(NYC_CSCL_PATH)
        header_table = table.first()
        # start testing
        res = sc.textFile(NYC_CSCL_PATH) \
                .filter(lambda x: x != header_table) \
                .mapPartitions(lambda x: csv.reader(x)) \
                .filter(lambda x: len(x) >= 30) \
                .map(lambda x: (x[0], (x[2], x[3], x[4], x[5], x[10], x[13], x[28]))) \
                .reduceByKey(lambda x, y: x) \
                .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6])) \
                .collect()
        assert len(res) == 119801
        lookup = construct_lookup(res)
        assert len(lookup) == 18069
        num_total = 0
        for v in lookup.values():
            num_total += len(v)
        assert num_total == 239602
        LOOKUP_BCAST = sc.broadcast(lookup)
        assert len(LOOKUP_BCAST) == len(lookup)

    def test_output(self):
        table = sc.textFile(NYC_CSCL_PATH)
        header_table = table.first()
        # start testing
        res = sc.textFile(NYC_CSCL_PATH) \
                .filter(lambda x: x != header_table) \
                .mapPartitions(lambda x: csv.reader(x)) \
                .filter(lambda x: len(x) >= 30) \
                .map(lambda x: (x[0], (x[2], x[3], x[4], x[5], x[10], x[13], x[28]))) \
                .reduceByKey(lambda x, y: x) \
                .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6])) \
                .collect()
        assert len(res) == 119801
        lookup = construct_lookup(res)
        print(lookup[('tides la', 5)])
        print(lookup[('tides ln', 5)])
        print(lookup[('roosevelt is br ped & bike path', 4)])
        print(lookup[('silver ct', 5)])
        print(lookup[('bluh, bluh, dummy', 1)])

    def test_lookup_street_segment(self):
        table = sc.textFile(NYC_CSCL_PATH)
        header_table = table.first()
        # start testing
        res = sc.textFile(NYC_CSCL_PATH) \
                .filter(lambda x: x != header_table) \
                .mapPartitions(lambda x: csv.reader(x)) \
                .filter(lambda x: len(x) >= 30) \
                .map(lambda x: (x[0], (x[2], x[3], x[4], x[5], x[10], x[13], x[28]))) \
                .reduceByKey(lambda x, y: x) \
                .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6])) \
                .collect()
        assert len(res) == 119801
        lookup = construct_lookup(res)
        print(lookup_street_segment([1, 5, '17', 'silver ct'], lookup))
        print(lookup_street_segment([1, 1, '17', 'silver ct'], lookup))
        print(lookup_street_segment([1, 5, '', 'silver ct'], lookup))
        print(lookup_street_segment([1, 5, '59', 'silver ct'], lookup))
        print(lookup_street_segment([1, 5, '18', 'silver ct'], lookup))
        print(lookup_street_segment([1, 5, '21', 'silver ct'], lookup))
