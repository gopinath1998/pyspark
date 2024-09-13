from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("min_temp_station_rdd")

sc=SparkContext(conf=conf)

def parseLine(line):
    fields=line.split(",")
    stationID=fields[0]
    entryType=fields[2]
    temp=float(fields[3])*0.1*(9.0/5.0)+32.0

    return (stationID,entryType,temp)


lines=sc.textFile("c:/SparkCourse/1800.csv")

parsed_lines=lines.map(parseLine)

min_temps=parsed_lines.filter(lambda x: "TMIN" in x[1])
station_temp=min_temps.map( lambda x :(x[0],x[2]))
min_temps=station_temp.reduceByKey( lambda x,y : min(x,y))

results=min_temps.collect()

for result in results:
    print(result[0]+"\t {:.2f}F".format(result[1]))
