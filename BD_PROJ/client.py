from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import *

def playdels_update(match,event):
    pass
def chems_update(match):
    pass

def driver(rdd):
    a = rdd.collect()
    print(type(a))
    match = json.loads(a[0])
    for event in a[1:]:
        event = json.loads(event)
        playdels_update(match,event)
    chems_update(match)

spark = SparkSession.builder.appName("Proj").master("local[2]").getOrCreate()

schema2 = StructType([
    
    StructField('player_1', IntegerType(), True),
    StructField('player_2', IntegerType(), True),
    StructField('chemistry', IntegerType(), True),
]) 

schema = StructType([
    
    StructField('match_date', IntegerType(), True),
    StructField('team_1', IntegerType(), True),
    StructField('team_2', IntegerType(), True),
    StructField('score_1', IntegerType(), True),
    StructField('score_2', IntegerType(), True),
    
    StructField('player_Id', IntegerType(), True),

    StructField('accurate_pass', IntegerType(), True),
    StructField('not_accurate_pass', IntegerType(), True),
    StructField('key_pass', IntegerType(), True),
    
    StructField('duels_lost', IntegerType(), True),
    StructField('duels_neutral', IntegerType(), True),
    StructField('duels_won', IntegerType(), True),
    
    StructField('no_of_effective_freekicks', IntegerType(), True),
    StructField('no_of_penalties_scored', IntegerType(), True),
    StructField('total_no_of_freekicks', IntegerType(), True),
    
    StructField('shot_is_on_target', IntegerType(), True),
    StructField('shot_is_not_on_target', IntegerType(), True),
    StructField('shot_was_a_goal', IntegerType(), True),
    
    StructField('no_of_fouls', IntegerType(), True),
    
    StructField('no_of_own_goals', IntegerType(), True),
    
    StructField('no_of_goals', IntegerType(), True),
    
    StructField('minutes_played', IntegerType(), True),
    
    StructField('yellow_card', IntegerType(), True),
    StructField('red_card', IntegerType(), True),
    
  ])

playdels = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
playdels.printSchema()


chems = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema2)
chems.printSchema()


ssc = StreamingContext(spark.sparkContext, 5)
dstream = ssc.socketTextStream('localhost', 6100)
dstream.foreachRDD(driver)
ssc.start()
ssc.awaitTermination()
