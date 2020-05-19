from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .appName('BDA Final Project') \
                        .master('local') \
                        .getOrCreate()
    data = [
        (0, 0, 0, 0, 0, 0.8),
        (0, 0, 0, 0, 1, 0.2),
        (1, 0, 0, 0, 0, 0.1),
    ]
    rdd = spark.sparkContext.parallelize(data)
    spark_df = spark.createDataFrame(rdd)
    spark_df.write.csv('test_final.csv')
