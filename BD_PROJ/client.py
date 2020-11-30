from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# to_datetime =  udf (lambda x: datetime.strptime(x, '%B %d, %Y at %I:%M:%S %p %Z%z'), DateType())
# to_datetime =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), DateType())


# def to_datetime(x):
#     tempdate = to_timestamp(x.split()[0],"yyyy-MM-dd")
#     return tempdate

def select_this_match(req_match):
    ret = playdels.filter(req_match['dateutc']==playdels.match_date).filter(req_match['label']==playdels.label)
    return ret

def return_pass_accuracy(req_player_id,match=0):
    if(match):
        np = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'normal_pass':'sum'}).collect()[0]['sum(normal_pass)']
        anp = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'accurate_normal_pass':'sum'}).collect()[0]['sum(accurate_normal_pass)']
        kp = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'key_pass':'sum'}).collect()[0]['sum(key_pass)']
        akp = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'accurate_key_pass':'sum'}).collect()[0]['sum(accurate_key_pass)']
    else :
        np = playdels.filter(playdels.player_Id==req_player_id).agg({'normal_pass':'sum'}).collect()[0]['sum(normal_pass)']
        anp = playdels.filter(playdels.player_Id==req_player_id).agg({'accurate_normal_pass':'sum'}).collect()[0]['sum(accurate_normal_pass)']
        kp = playdels.filter(playdels.player_Id==req_player_id).agg({'key_pass':'sum'}).collect()[0]['sum(key_pass)']
        akp = playdels.filter(playdels.player_Id==req_player_id).agg({'accurate_key_pass':'sum'}).collect()[0]['sum(accurate_key_pass)']
    return (anp+(akp*2))/(np+(kp*2))

def get_duel_effect(req_player_id,match=0):
    if (match):
        duels_won = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'duels_won':'sum'}).collect()[0]['sum(duels_won)']
        duels_neutral = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'duels_neutral':'sum'}).collect()[0]['sum(duels_neutral)']
        duels_lost = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'duels_neutral':'sum'}).collect()[0]['sum(duels_neutral)']
    else:
        duels_won = playdels.filter(playdels.player_Id==req_player_id).agg({'duels_won':'sum'}).collect()[0]['sum(duels_won)']
        duels_neutral = playdels.filter(playdels.player_Id==req_player_id).agg({'duels_neutral':'sum'}).collect()[0]['sum(duels_neutral)']
        duels_lost = playdels.filter(playdels.player_Id==req_player_id).agg({'duels_neutral':'sum'}).collect()[0]['sum(duels_neutral)']
    return (duels_won + duels_neutral*0.5)/(duels_won + duels_neutral + duels_lost)

def return_free_kick_effectiveness(req_player_id,match=0):
    if (match) :
        eff_freekicks = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'no_of_effective_freekicks':'sum'}).collect()[0]['sum(no_of_effective_freekicks)']
        penalties_scored = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'no_of_penalties_scored':'sum'}).collect()[0]['sum(no_of_penalties_scored)']
        total_freekicks = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'total_no_of_freekicks':'sum'}).collect()[0]['sum(total_no_of_freekicks)']
    else : 
        eff_freekicks = playdels.filter(playdels.player_Id==req_player_id).agg({'no_of_effective_freekicks':'sum'}).collect()[0]['sum(no_of_effective_freekicks)']
        penalties_scored = playdels.filter(playdels.player_Id==req_player_id).agg({'no_of_penalties_scored':'sum'}).collect()[0]['sum(no_of_penalties_scored)']
        total_freekicks = playdels.filter(playdels.player_Id==req_player_id).agg({'total_no_of_freekicks':'sum'}).collect()[0]['sum(total_no_of_freekicks)']
    return (eff_freekicks+penalties_scored)/(total_freekicks)

def get_shot_effect(req_player_id,match=0):
    if(match):
        shot_on_target_and_goal = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_goal':'sum'}).collect()[0]['sum(shot_on_target_and_goal)']
        shot_on_target_and_not_goal = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_not_goal':'sum'}).collect()[0]['sum(shot_on_target_and_not_goal)']
        total_shots = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'total_shots':'sum'}).collect()[0]['sum(total_shots)']
    else:
        shot_on_target_and_goal = playdels.filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_goal':'sum'}).collect()[0]['sum(shot_on_target_and_goal)']
        shot_on_target_and_not_goal = playdels.filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_not_goal':'sum'}).collect()[0]['sum(shot_on_target_and_not_goal)']
        total_shots = playdels.filter(playdels.player_Id==req_player_id).agg({'total_shots':'sum'}).collect()[0]['sum(total_shots)']
    return (shot_on_target_and_goal + shot_on_target_and_not_goal*0.5)/(total_shots)

def return_no_of_fouls(req_player_id):
    no_of_fouls = playdels.filter(playdels.player_Id==req_player_id).agg({'no_of_fouls':'sum'}).collect()[0]['sum(no_of_fouls)']
    return no_of_fouls

def get_own_goals(req_player_id):
    no_of_own_goals = playdels.filter(playdels.player_Id==req_player_id).agg({'no_of_own_goals':'sum'}).collect()[0]['sum(no_of_own_goals)']
    return no_of_own_goals

def playdels_update(match,event):
    pass
def chems_update(match):
    pass

def driver(rdd):
    a = rdd.collect()
    print(type(a[0]))
    match = json.loads(a[0])
    # print(json.dumps(match, indent = 3)) 
    print("Pass accracy",return_pass_accuracy(1))
    print("Free Kick effectiveness",return_free_kick_effectiveness(1))
    print("No of Fouls",return_no_of_fouls(1))
    for event in a[1:]:
        print("-")
        break
    chems_update(match)

spark = SparkSession.builder.appName("Proj").master("local[2]").getOrCreate()

schema2 = StructType([
    StructField('player_1', IntegerType(), True),
    StructField('player_2', IntegerType(), True),
    StructField('chemistry', IntegerType(), True),
]) 

schema = StructType([
    StructField('match_date', StringType(), True),
    StructField('label', StringType(), True),
    
    StructField('player_Id', IntegerType(), True),
    StructField('team_Id', IntegerType(), True),

    StructField('normal_pass', IntegerType(), True),
    StructField('accurate_normal_pass', IntegerType(), True),
    StructField('key_pass', IntegerType(), True),
    StructField('accurate_key_pass', IntegerType(), True),

    StructField('duels_lost', IntegerType(), True),
    StructField('duels_neutral', IntegerType(), True),
    StructField('duels_won', IntegerType(), True),
    
    StructField('no_of_effective_freekicks', IntegerType(), True),
    StructField('no_of_penalties_scored', IntegerType(), True),
    StructField('total_no_of_freekicks', IntegerType(), True),
    
    StructField('shot_on_target_and_goal', IntegerType(), True),
    StructField('shot_on_target_and_not_goal', IntegerType(), True),
    StructField('total_shots', IntegerType(), True),
    
    StructField('no_of_fouls', IntegerType(), True),
    
    StructField('no_of_own_goals', IntegerType(), True),
    
    StructField('no_of_goals', IntegerType(), True),
    
    StructField('minutes_played', IntegerType(), True),
    
    StructField('yellow_card', IntegerType(), True),
    StructField('red_card', IntegerType(), True),
    
    StructField('venue', StringType(), True),
    StructField('gameweek', IntegerType(), True),
    StructField('duration', StringType(), True),
  ])

playdels = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
# # playdels.printSchema()


chems = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema2)
# # chems.printSchema()






playdels_cols = list(playdels.columns)
newRow = spark.createDataFrame([('2017-08-11 18:45:00','Arsenal - Leicester City, 4 - 3',1,2,3,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,'b',2,'b'),('2017-08-11 18:45:00','Arsenal - Leicester City, 4 - 3',1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,'b',1,'b'),('2017-08-11 18:45:00','2-2',1,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,'b',3,'b')], playdels_cols)

playdels = playdels.union(newRow)


# print("Pass accracy",return_pass_accuracy(1))
# print("Free Kick effectiveness",return_free_kick_effectiveness(1))
# print("No of Fouls",return_no_of_fouls(1))

playdels.show()








ssc = StreamingContext(spark.sparkContext, 5)
dstream = ssc.socketTextStream('localhost', 6100)
dstream.foreachRDD(driver)
ssc.start()
ssc.awaitTermination()
