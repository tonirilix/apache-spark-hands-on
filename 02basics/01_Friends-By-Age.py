from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# We get all the data
lines = sc.textFile("01_fakefriends.csv")

# Extract age and num of friends as a list of tuples
rdd = lines.map(parseLine)

# First to every value we create a key pair of (value, 1)
# Then we reduce by age and create a key pair of (sum(value), sum (1)).
# With that we will know the sum of friends per same age to then get the average
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# we get the average by mapping new values
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# get the last modified list
results = averagesByAge.collect()

# Sort data by keys
sortedResults = collections.OrderedDict(sorted(results, key=lambda t: t[0]))

# Loop data to print it
for result in sortedResults.iteritems():
    print result
