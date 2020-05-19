from pyspark import SparkContext
from pyspark.sql import SparkSession

import csv
import os
import datetime as dt
import re


NYC_CSCL_PATH = 'data\\nyc_cscl.csv'
root = 'test'
violation_records = [os.path.join(root, 'violation_small1.csv'),
                     os.path.join(root, 'violation_small2.csv')]
VIOLATION_PATH = ','.join(violation_records)
# indices for lookup table
# PHYSICALID = 0
# L_LOW_HN = 1
# L_HIGH_HN = 2
# R_LOW_HN = 3
# R_HIGH_HN = 4
# ST_LABEL = 5
# BOROCODE_IDX = 6
# FULL_STREE = 7


def match_house_number(hn_record, segment):
    # exclude single character house numbers
    if len(hn_record) == 1 and (not hn_record.isnumeric()):
        return False
    # if a record is empty, assigns 0
    if len(hn_record) == 0:
        hn_record = 0
    # otherwise concatenate two values together
    # example: '187-09' = 18709 <int>
    # example: '187' = 187 <int>
    else:
        hn_record = re.sub('\s', '-', hn_record)
        try:
            hn_record = int(hn_record.replace('-', ''))
        except ValueError:
            return False
    # format house numbers in lookup segment in the same way
    # if hn_record is even, we should use 'R'; otherwise, 'L'
    if hn_record%2 == 0:
        if len(segment[3]) == 0:
            lower = 0
        else:
            lower = int(re.sub('-0|-', '', segment[3]))
        if len(segment[4]) == 0:
            high = 0
        else:
            high = int(re.sub('-0|-', '', segment[4]))
    else:
        if len(segment[1]) == 0:
            lower = 0
        else:
            lower = int(re.sub('-0|-', '', segment[1]))
        if len(segment[2]) == 0:
            high = 0
        else:
            high = int(re.sub('-0|-', '', segment[2]))
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


def street_segmentid_lookup(HN, STREET_NAME, BOROCODE, physicalID_list):
    for segment in physicalID_list:
        street = STREET_NAME.lower()
        # print(type(int(segment['BOROCODE'])), type(v_record['Violation County']))
        # first check county code and street name
        if (BOROCODE == int(segment[6])) and \
           ((street == segment[7].lower()) or (street == segment[5].lower())):
           # then, check house number: odd number is stored in left
           if match_house_number(HN, segment):
                return segment[0]
    # returns -1 if there is no match
    return -1


def export_csv(output, lookup_table, path):
    """ Export output in csv format """
    # build lookup table with counts
    physicalIDs = {}
    for row in lookup_table:
        if row[0].isnumeric():
            id = int(row[0])
            physicalIDs.update({id:[0, 0, 0, 0, 0, 0]})
    # assign the count in output
    for out in output:
        try:
            lookup = int(out[0])
            for idx in range(6):
                physicalIDs[lookup][idx] = out[1][idx][1]
        except KeyError:
            pass
    # export the resutl as csv
    data = []
    for key in sorted(physicalIDs.keys()):
        data.append(tuple([key] + physicalIDs[key]))
    spark = SparkSession.builder \
                        .appName('BDA Final Project') \
                        .master('local') \
                        .getOrCreate()
    rdd = spark.sparkContext.parallelize(data)
    spark_df = spark.createDataFrame(rdd)
    spark_df.write.csv(path)


def ols(data):
    """ data = [(x1, y1), ..., (xi, yi), ..., (xN, yN)] """
    x_bar = sum([d[0] for d in data])/len(data)
    y_bar = sum([d[1] for d in data])/len(data)
    numerator = sum([(d[0] - x_bar)*(d[1] - y_bar) for d in data])
    denomenator = sum([(d[0] - x_bar)**2 for d in data])
    if denomenator == 0:
        return 0
    else:
        return numerator/denomenator


def fill_zer0(row):
    expected = {2015: 0, 2016:0, 2017:0, 2018:0, 2019:0}
    for x in row:
        expected[x[0]] += x[1]
    expected = [(k, v) for k, v in expected.items()]
    return expected


if __name__ == '__main__':
    sc = SparkContext()
    table = sc.textFile(NYC_CSCL_PATH)
    header_table = table.first()
    # start testing
    lookup = sc.textFile(NYC_CSCL_PATH) \
               .filter(lambda x: x != header_table) \
               .mapPartitions(lambda x: csv.reader(x)) \
               .filter(lambda x: len(x) >= 30) \
               .map(lambda x: (x[0], (x[2], x[3], x[4], x[5], x[10], x[13], x[28]))) \
               .reduceByKey(lambda x, y: x) \
               .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6])) \
               .collect()
    LOOKUP_BCAST = sc.broadcast(lookup)
    # skip headers
    data = sc.textFile(VIOLATION_PATH)
    header_data = data.first()
    # load data
    res = sc.textFile(VIOLATION_PATH) \
            .filter(lambda x: x != header_data) \
            .mapPartitions(lambda x: csv.reader(x)) \
            .filter(lambda x: len(x) >= 25) \
            .map(lambda x: (int(dt.datetime.strptime(x[4], '%m/%d/%Y').year), x[21], x[23], x[24])) \
            .filter(lambda x: (2015 <= x[0] and x[0] <= 2019)) \
            .map(lambda x: (x[0], countyname2borocode(x[1]), x[2], x[3])) \
            .filter(lambda x: x[1] > 0) \
            .map(lambda x: (x[0], street_segmentid_lookup(x[2], x[3], x[1], LOOKUP_BCAST.value))) \
            .filter(lambda x: int(x[1]) > 0) \
            .map(lambda x: ((x[1], x[0]), 1)) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[0][0], [(x[0][1], x[1])])) \
            .reduceByKey(lambda x, y: x + y) \
            .mapValues(lambda x: fill_zer0(x) + [('OLS_COEF', ols(x))]) \
            .collect()
    export_csv(res, LOOKUP_BCAST.value, 'test.csv')
