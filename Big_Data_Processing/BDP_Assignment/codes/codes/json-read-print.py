# this program reads data from json format and then prints the schema of json
import sys
import re
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: .py <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("read-json")\
        .getOrCreate()

    dataframe = spark.read.json(sys.argv[1])
    dataframe.printSchema()
    rd=dataframe.select("text").rdd
    rd.saveAsTextFile("outfile")
