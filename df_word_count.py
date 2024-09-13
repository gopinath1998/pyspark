from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark=SparkSession.builder.appName("WordCount").master("local[*]").getOrCreate()

text=spark.read.text("c://SparkCourse/book.txt")

words=text.select(func.explode(func.split(text.value,"\\W+")).alias("word"))

better_words=words.filter(words.word != "")

lower_words=better_words.select(func.lower(better_words.word).alias("word"))

word_counts=lower_words.groupby(lower_words.word).count()

##desc(col_name) used to sort the df in descending order on that specified column
word_count_sorted=word_counts.sort(func.desc("count"))

word_count_sorted.show(word_count_sorted.count())

