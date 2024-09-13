from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("wordcount")
sc=SparkContext(conf=conf)

input=sc.textFile("C:/SparkCourse/Book.txt")

words=input.flatMap( lambda x: x.split())

word_map=words.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)

results=word_map.map( lambda x : (x[1],x[0])).sortByKey().collect()

for res in results:
    print( res[1]+"\t"+str(res[0]))



