from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("02_1800.csv")

# Transform data to a (stationID, entryType, temperature) list
parsedLines = lines.map(parseLine)

# Only get TMIN values
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# Transform to (stationID, temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# Get the min temperature per station
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

# collect
results = minTemps.collect()

# print
for result in results:
    print result[0] + "\t{:.2f}F".format(result[1])
