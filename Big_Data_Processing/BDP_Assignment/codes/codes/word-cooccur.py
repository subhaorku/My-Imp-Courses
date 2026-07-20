# this program computes word-cooccurrence based on how often they are appearinn in the same sentence
import sys
import re
from operator import add

from pyspark.sql import SparkSession

# function to generate all pairs of words from a sentence
def getPairs(line: str) -> str:
  res = re.split('\W+', line.lower())
  res.sort()
  l = len(res)
  allp =""
  for i in range(0,l-1):
    for j in range(i+1,l):
      allp=allp+res[i]+","+res[j]+"#"
  return allp

# the main function 
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: <file>", file=sys.stderr)
        sys.exit(-1)

# creates spark session to start executing spark  code
    spark = SparkSession\
        .builder\
        .appName("word-cooccurrence")\
        .getOrCreate()

    # reads text
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])  
    # creates dataframe and counts word co-occurrences
    cols = ["word1,word2","count"]
    wpairs=lines.map(lambda p: getPairs(p)).flatMap(lambda s: s.split("#")).map(lambda x: (x, 1)).reduceByKey(add).toDF(cols)
    wpairs.show(truncate=False)
    #wpairs.saveAsTextFile("outfile")
    spark.stop()
