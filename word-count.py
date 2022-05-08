from pyspark.sql import SparkSession

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  spark,sc = init_spark()
  nums = sc.parallelize([1,2,3,4])
  print(nums.map(lambda x: x*x).collect())
  print("Application Completed !!!")

"""
def main():
  print("Application Started !!!")
  spark = SparkSession.builder.appName("word-count").getOrCreate()
  sc = spark.sparkContext
  data = sc.textFile("file://C//All_Projects//all_python_projects//python-basic-poc//data//data.txt")
  # read data from text file and split each line into words
  words = data.flatMap(lambda line: line.split(" "))

  # count the occurrence of each word
  word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
  print word_counts
  #word_counts.t
"""