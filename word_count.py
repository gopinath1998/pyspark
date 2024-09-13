from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("wordcount")
sc=SparkContext(conf=conf)

input=sc.textFile("C:/SparkCourse/Book.txt")

words=input.flatMap( lambda x: x.split())

word_count=words.countByValue()

for word,count in word_count.items():
    clean_word=word.encode('ascii','ignore')
    if clean_word:
        print (word,count)