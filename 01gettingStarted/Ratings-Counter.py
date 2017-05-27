# import os
# import sys
#
# # Path for spark source folder
# os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/1.6.1"
#
# # Append pyspark  to Python Path
# sys.path.append("/usr/local/Cellar/apache-spark/1.6.1/libexec/python")
# sys.path.append("/usr/local/Cellar/apache-spark/1.6.1/libexec/python/lib/py4j-0.9-src.zip")

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i" % (key, value)
