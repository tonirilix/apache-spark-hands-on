import re
from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    productPrice = float(fields[2])
    return (customerId, productPrice)

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("04_1_customer-orders.csv")
parsedLines = lines.map(parseLine)
sumarized = parsedLines.reduceByKey(lambda x, y: x + y)
wordCountsSorted = sumarized.map(lambda (x,y): (y,x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    price = str(result[0])
    customer = str(result[1])
    print price + ":\t\t" + customer
