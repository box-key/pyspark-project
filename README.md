Description
-----------
This is an archive of code for pyspark project in my Big Data Analytics class. The goal of the project is to compute the number of parking violations for each street segment in NYC for each year from 2015 to 2019. The program also computes the change rate of parking violation by ordinary least squares over years.

The datasets can be found at [Parking Violation Records](https://data.cityofnewyork.us/browse?q=%22Parking%20Violations%20Issued%22) and [NYC Street Segment](https://data.cityofnewyork.us/City-Government/NYC-Street-Centerline-CSCL-/exjm-f27b).

Performance
-----------
The size of parking violation datasets between 2015 and 2019 is 10GB. My program takes about 3 minutes and 30 seconds to complete the task with 6 executors, 5 executor cores and 10GB executor memory on a hadoop cluster. It outputs the result in a csv format on hdfs.
