from pyspark import SparkContext
from pyspark.sql import SparkSession

from collections import defaultdict

import csv
import datetime as dt
import re

import sys


NYC_CSCL_PATH = 'hdfs:///.../nyc_cscl.csv'
VIOLATION_PATH = 'hdfs:///.../nyc_parking_violation/*.csv'


## Indices for lookup table
# PHYSICALID = 0
# L_LOW_HN = 1
# L_HIGH_HN = 2
# R_LOW_HN = 3
# R_HIGH_HN = 4
# ST_LABEL = 5
# BOROCODE_IDX = 6
# FULL_STREE = 7
#
## Indices for violation record
# YEAR = 0
# SEGMENTID = 1
#
## lookup dictionary
# {(ST_LABEL, BOROCODE_IDX) : [((R_LOW_HN, R_HIGH_HN), (L_LOW_HN, L_HIGH_HN), PHYSICALID)]} * N +
# {(FULL_STREE, BOROCODE_IDX) : [((R_LOW_HN, R_HIGH_HN), (L_LOW_HN, L_HIGH_HN), PHYSICALID)]} * N
# , where N is the number of unique physicalIDs in the file.


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


def format_hn(hn_record):
    # if a record is empty, assigns 0
    if len(hn_record) == 0:
        return 0
    # concatenate two values together
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


def export_csv(output, lookup_table, path):
    """ Export output in csv format """
    # build lookup table with counts
    physicalIDs = defaultdict(list)
    for row in lookup_table.values():
    # row = [(.., .., physical ID), (.., .., physical ID), ..., (.., .., physical ID)]
        for element in row:
            id = element[2]
            physicalIDs[id] = [0, 0, 0, 0, 0, 0]
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
    spark_df.write.mode('overwrite').csv(path)


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
    # initialize spark context
    sc = SparkContext()
    # read nyc_cscl.csv and skip headers
    table = sc.textFile(NYC_CSCL_PATH)
    header_table = table.first()
    """ Steps
    1. read csv
    2. skip header
    3. filter out rows with less than 30 elements to avoid index error
    4. map input by physicalID as key to remove duplicates
    5. group row inputs into a tuple
    6. obtain a list of tuples, then construct lookup table using defaultdict
    """
    res = sc.textFile(NYC_CSCL_PATH) \
            .filter(lambda x: x != header_table) \
            .mapPartitions(lambda x: csv.reader(x)) \
            .filter(lambda x: len(x) >= 30) \
            .map(lambda x: (x[0], (x[2], x[3], x[4], x[5], x[10], x[13], x[28]))) \
            .reduceByKey(lambda x, y: x) \
            .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6])) \
            .collect()
    lookup = construct_lookup(res)
    LOOKUP_BCAST = sc.broadcast(lookup)
    # read parking violations and skip headers
    data = sc.textFile(VIOLATION_PATH)
    header_data = data.first()
    """ Steps
    1. read csv
    2. skip header
    3. filter out rows with less than 25 elements to avoid index error
    4. filter out rows whose Issue date doesn't follow the datetime format, i.e. %m/%d/%Y
    5. map inputs by issue year by key and issue county, house number and street name
    6. filter out violation records that were not issued between 2015 and 2019
    7. assign borocode based on county name (if no match, returned values will be -1)
    8. filter out rows where borocode is -1
    9. find street segment id based on borocode, house number and street name using the lookup table
       (if no match, returned values will be -1)
    10. filter out rows where street segment id is -1
    11. map violation records by issued year and street segment id as keys and 1 as value
    12. count the number of violations for pairs of issued year and street segment id
    13. compute the coefficient of the OLS and fill out missing values with 0
        e.g. [('2015', 1), ('2017', 10)] => [('2015', 1), ('2016', 0), ('2017', 10) ...]
    14. collect the values and export to csv file
    """
    res = sc.textFile(VIOLATION_PATH) \
            .filter(lambda x: x != header_data) \
            .mapPartitions(lambda x: csv.reader(x)) \
            .filter(lambda x: len(x) >= 25) \
            .filter(lambda x: x[4].find('/') >= 0) \
            .map(lambda x: (int(dt.datetime.strptime(x[4], '%m/%d/%Y').year), x[21], x[23], x[24])) \
            .filter(lambda x: (2015 <= x[0] and x[0] <= 2019)) \
            .map(lambda x: (x[0], countyname2borocode(x[1]), x[2], x[3])) \
            .filter(lambda x: x[1] > 0) \
            .map(lambda x: (x[0], lookup_street_segment(x, LOOKUP_BCAST.value))) \
            .filter(lambda x: int(x[1]) > 0) \
            .map(lambda x: ((x[1], x[0]), 1)) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[0][0], [(x[0][1], x[1])])) \
            .reduceByKey(lambda x, y: x + y) \
            .mapValues(lambda x: fill_zer0(x) + [('OLS_COEF', ols(x))]) \
            .collect()
    # export output to csv
    export_csv(res, LOOKUP_BCAST.value, sys.argv[1])
