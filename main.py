from pyspark import SparkContext

import csv
import datetime as dt
import re
import os


NYC_CSCL_PATH = 'data\\nyc_cscl.csv'
root = 'test'
violation_records = [os.path.join(root, 'violation_small1.csv'),
                     os.path.join(root, 'violation_small2.csv')]
VIOLATION_RECORDS_PATH = ','.join(violation_records)


def match_house_number(hn_record, segment):
    # exclude single character house numbers
    if len(hn_record) == 1 and (not hn_record.isnumeric()):
        return False
    # exlude cases like 789A
    if (hn_record.find('-') == -1) and (not hn_record.isnumeric()):
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


def street_segmentid_lookup(HN, STREET_NAME, BOROCODE, physicalID_list):
    for segment in physicalID_list:
        street = STREET_NAME.lower()
        # print(type(int(segment['BOROCODE'])), type(v_record['Violation County']))
        # first check county code and street name
        if (BOROCODE == int(segment['BOROCODE'])) and \
           ((street == segment['FULL_STREE'].lower()) or (street == segment['ST_LABEL'].lower())):
           # then, check house number: odd number is stored in left
           if match_house_number(HN, segment):
                return segment['PHYSICALID']
    # returns -1 if there is no match
    return -1


def export_csv(output):
    """ Export output in csv format """
    # build lookup table with counts
    with open(NYC_CSCL_PATH, 'r') as f:
        file = csv.DictReader(f)
        physicalIDs = {}
        for row in file:
            id = int(row['PHYSICALID'])
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
    with open('temp.csv', 'w', newline='\n') as f:
        writer = csv.writer(f)
        for k, v in physicalIDs.items():
            writer.writerow([k] + v)

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
    # load lookup table
    with open(NYC_CSCL_PATH, 'r') as f:
        file = csv.DictReader(f)
        lookup = [row for row in file]
    # broadcast lookup table
    LOOKUP_BCAST = sc.broadcast(lookup)
    # skip headers
    data = sc.textFile(VIOLATION_RECORDS_PATH)
    header = data.first()
    # load data
    res = sc.textFile(VIOLATION_RECORDS_PATH) \
            .filter(lambda x: x != header) \
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
            .sortByKey(True, 1) \
            .map(lambda x: (x[0][0], [(x[0][1], x[1])])) \
            .reduceByKey(lambda x, y: x + y) \
            .mapValues(lambda x: fill_zer0(x) + [('OLS_COEF', ols(x))]) \
            .collect()
    export_csv(res)
