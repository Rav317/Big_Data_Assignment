from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import time
start = time.process_time()
# your code here    



word_given=sys.argv[1]
k=int(sys.argv[2])
hdfs_path1=sys.argv[3]
hdfs_path2=sys.argv[4]

spark= SparkSession.builder.appName("Assignment3").getOrCreate()
shapes = spark.read.option("header",True).csv(hdfs_path1)
# shapes = spark.read.option("header",True).csv(hdfs_path1).repartition("key_id")
shapes_stat = spark.read.option("header",True).csv(hdfs_path2).repartition("key_id")
# shapes_stat = spark.read.option("header",True).csv(hdfs_path2)
# print("shape",shapes.rdd.getNumPartitions())
# print("shape-stat",shapes_stat.rdd.getNumPartitions())

merged=shapes.join(shapes_stat,shapes.key_id == shapes_stat.key_id,'left').select(shapes.word,shapes.key_id,shapes.countrycode,shapes_stat.recognized,shapes_stat.Total_Strokes)
filtered=merged.filter(col('word')==word_given).filter(col('recognized') == False).filter(col('Total_Strokes') < k)
final=filtered.groupBy(col('countrycode')).count()
final=final.sort(final.countrycode.asc())

# final.show()
if(final.count()==0):
	print(0)
else:
	for row in final.rdd.collect():
	    print(row[0],",",row[1],sep="")

print("Time-",time.process_time() - start)


# command to run
# python3 '/home/mohit/Big_Data_Assignment/Assignment_3/task2.py' asparagus 1 hdfs://localhost:9000/dataset1 hdfs://localhost:9000/dataset2
