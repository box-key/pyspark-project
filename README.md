Description
-----------
This is an archive of code for pyspark project in my Big Data Analytics class. The goal of the project is to compute the number of parking violations for each street segment in NYC for each year from 2015 to 2019. The program also computes the change rate of parking violation over years by ordinary least square.

The datasets can be found at [Parking Violation Records](https://data.cityofnewyork.us/browse?q=%22Parking%20Violations%20Issued%22) and [NYC Street Segment](https://data.cityofnewyork.us/City-Government/NYC-Street-Centerline-CSCL-/exjm-f27b).

What it does?
-------------
1. Read NYC street segment ID and construct a lookup table to assign street segment ID to parking violation records. 
> The table is a default dictionary where street name and borough are "key" and a list of Segment ID and house number range associated with key is "value". 
2. Broadcast the lookup table.
2. Read parking violation records and remove rows that are improperly formatted.
2. Map coutyname to borough code.
2. Assign street segment ID to parking violation records based on their borough code, street name and house number.
2. Map violation records by street segment ID and issue year as keys and 1 as value.
> pass 1 to count the number of violations per pair of segment ID and issue year.
7. Compute the change rate of parking violations over years by Ordinary Least Square.
7. Export the output to a csv file.

Performance
-----------
The size of parking violation datasets between 2015 and 2019 is 10GB. My program takes about 3 minutes and 30 seconds to complete the task with 6 executors, 5 executor cores and 10GB executor memory on a hadoop cluster. It outputs the result in a csv format on hdfs.
