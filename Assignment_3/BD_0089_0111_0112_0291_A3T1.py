from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg
import sys



word_given=sys.argv[1]
# k=int(sys.argv[2])
hdfs_path1=sys.argv[2]
hdfs_path2=sys.argv[3]


spark= SparkSession.builder.appName("Assignment3").getOrCreate()
shapes = spark.read.option("header",True).csv(hdfs_path1)
shapes_stat = spark.read.option("header",True).csv(hdfs_path2)

filtered=shapes_stat.filter(col('word')==word_given)
rec=filtered.filter(col('recognized')==True).groupBy(col('word'))
unrec=filtered.filter(col('recognized')==False).groupBy(col('word'))

final_rec=rec.agg({'Total_Strokes':'avg'})
final_unrec=unrec.agg({'Total_Strokes':'avg'})

# rec.show()
# unrec.show()
for i in final_rec.rdd.collect():
	print("{:.5f}".format(i[1]))

for i in final_unrec.rdd.collect():
	print("{:.5f}".format(i[1]))



# command to run
# python3 '/home/mohit/Big_Data_Assignment/Assignment_3/BD_0089_0111_0112_0291_A3T1.py' asparagus hdfs://localhost:9000/dataset1 hdfs://localhost:9000/dataset2
