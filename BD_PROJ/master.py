from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans

import pandas as pd
import csv
import sys
import json
from datetime import datetime


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
	StructField('is_substituted', IntegerType(), True),
	
	StructField('yellow_card', IntegerType(), True),
	StructField('red_card', IntegerType(), True),
	
	StructField('venue', StringType(), True),
	StructField('gameweek', IntegerType(), True),
	StructField('duration', StringType(), True),
  ])

playdels = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)

# converts the datetime string into datetime object
def to_datetime(x):
	tempdate = to_timestamp(x.split()[0],"yyyy-MM-dd")
	return tempdate

# returns the match details based on the match identifiers
def select_this_match(req_match):
	ret = playdels.filter(req_match['dateutc'].split()[0]==playdels.match_date).filter(req_match['label']==playdels.label)
	return ret

# calculates the pass accuracy of the player for a particular match or aggregated
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

# calculates the duel effectiveness of the player for a particular match or aggregated
def return_duel_effectiveness(req_player_id,match=0):
	if (match):
		duels_won = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'duels_won':'sum'}).collect()[0]['sum(duels_won)']
		duels_neutral = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'duels_neutral':'sum'}).collect()[0]['sum(duels_neutral)']
		duels_lost = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'duels_neutral':'sum'}).collect()[0]['sum(duels_neutral)']
	else:
		duels_won = playdels.filter(playdels.player_Id==req_player_id).agg({'duels_won':'sum'}).collect()[0]['sum(duels_won)']
		duels_neutral = playdels.filter(playdels.player_Id==req_player_id).agg({'duels_neutral':'sum'}).collect()[0]['sum(duels_neutral)']
		duels_lost = playdels.filter(playdels.player_Id==req_player_id).agg({'duels_lost':'sum'}).collect()[0]['sum(duels_lost)']
	return (duels_won + duels_neutral*0.5)/(duels_won + duels_neutral + duels_lost)

# calculates the free kick effectiveness of the player for a particular match or aggregated
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

# calculates the number of shots on target of the player for a particular match or aggregated
def return_shots_on_target(req_player_id,match=0):
	if(match):
		shot_on_target_and_goal = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_goal':'sum'}).collect()[0]['sum(shot_on_target_and_goal)']
		shot_on_target_and_not_goal = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_not_goal':'sum'}).collect()[0]['sum(shot_on_target_and_not_goal)']
		# total_shots = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'total_shots':'sum'}).collect()[0]['sum(total_shots)']
		return shot_on_target_and_goal+shot_on_target_and_not_goal
	else:
		shot_on_target_and_goal = playdels.filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_goal':'sum'}).collect()[0]['sum(shot_on_target_and_goal)']
		shot_on_target_and_not_goal = playdels.filter(playdels.player_Id==req_player_id).agg({'shot_on_target_and_not_goal':'sum'}).collect()[0]['sum(shot_on_target_and_not_goal)']
		total_shots = playdels.filter(playdels.player_Id==req_player_id).agg({'total_shots':'sum'}).collect()[0]['sum(total_shots)']
	return (shot_on_target_and_goal + shot_on_target_and_not_goal*0.5)/(total_shots)

# calculates the number of fouls of the player for a particular match or aggregated
def return_no_of_fouls(req_player_id):
	no_of_fouls = playdels.filter(playdels.player_Id==req_player_id).agg({'no_of_fouls':'sum'}).collect()[0]['sum(no_of_fouls)']
	return no_of_fouls

# calculates the number of own goals of the player for a particular match or aggregated
def return_no_of_own_goals(req_player_id):
	no_of_own_goals = playdels.filter(playdels.player_Id==req_player_id).agg({'no_of_own_goals':'sum'}).collect()[0]['sum(no_of_own_goals)']
	return no_of_own_goals

# calculates the player contribution for a particular match using above attributes
def return_player_contribution(req_player_id,match):
	if(playdels.filter(playdels.player_Id==req_player_id).filter(playdels.label==match["label"]).collect()[0]['minutes_played']==0):
		return 0
	contribution = (return_pass_accuracy(req_player_id,match)+return_duel_effectiveness(req_player_id,match)+return_free_kick_effectiveness(req_player_id,match)+return_shots_on_target(req_player_id,match))/4
	if((select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['is_substituted'])!=0):
		contribution = 1.05*contribution
	else : 
		contribution = contribution*(playdels.filter(playdels.player_Id==req_player_id).filter(playdels.label==match["label"]).collect()[0]['minutes_played']/90)
	return contribution

# calculates the players performance using its contribution and penalties
def return_player_performance(req_player_id,match):
	contribution = return_player_contribution(req_player_id,match)
	foul_penalty = (0.005*playdels.filter(playdels.player_Id==req_player_id).filter(playdels.label==match["label"]).collect()[0]['no_of_fouls'])
	own_goal_penalty = (0.05*playdels.filter(playdels.player_Id==req_player_id).filter(playdels.label==match["label"]).collect()[0]['no_of_own_goals'])
	contribution = contribution - foul_penalty - own_goal_penalty
	return contribution

# calculates rating of player using performance
def return_player_rating(req_player_id,match):
	return (return_player_performance(req_player_id,match)+0.5)/2#player_profile.filter(player_profile.Id == req_player_id).collect()[0]['rating']

# add base values for attributes in playdel dataframe
def initialise_playdel(match):
	global playdels
	columns = list(playdels.columns)

	# getting attribute values from match json
	datee = match["dateutc"].split(" ")[0]
	label = match["label"]
	gameweek = match["gameweek"]
	venue = match["venue"]
	duration = match["duration"]

	# adding values for both teams in a match
	team_ids = list(match['teamsData'].keys())
	for t_id in team_ids:
		for players in match["teamsData"][t_id]["formation"]["lineup"]:
			playerId = players["playerId"]
			init_row = spark.createDataFrame([(datee, label, playerId, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 0, 0, 0, venue,gameweek,duration)], columns)
			playdels = playdels.union(init_row)

# calculates change in ratings for two players in a match
def rat_change(id1, id2, match):
	
	# getting rating after match and initial rating of the player
	mat_rat1 = return_player_rating(id_1, match)
	init_rat1 = player_profile.filter(player_profile.id == id_1).select(player_profile.rating)

	mat_rat2 = return_player_rating(id_2, match)
	init_rat2 = player_profile.filter(player_profile.id == id_2).select(player_profile.rating)

	rat_ch_1 = mat_rat1-init_rat1
	rat_ch_2 = mat_rat2-init_rat2

	ch = abs((rat_ch_1+rat_ch_2)/2)

	return rat_ch_1, rat_ch_2,ch

# updates the chemistries for all players played in a match
def chems_update(match):
	global chems
	global playdels
	team_ids = list(match['teamsData'].keys())

	team_1 = playdels.filter((playdels.match_date == match["dateutc"].split(" ")[0] )&( playdels.label == match["label"] )&( playdels.team_Id == team_ids[0])).select(playdels.player_Id)
	team_2 = playdels.filter((playdels.match_date == match["dateutc"].split(" ")[0] )&( playdels.label == match["label"] )&( playdels.team_Id == team_ids[1])).select(playdels.player_Id)

	# getting players for each team
	team_1_ids = [i['player_Id'] for i in team_1.collect()]
	team_2_ids = [i['player_Id'] for i in team_2.collect()]
	
	# calculating chemistries for players of opposite teams
	for pl_id_1 in team_1_ids:
		for pl_id_2 in team_2_ids:
			ch_1, ch_2,ch = rat_change(pl_id_1, pl_id_2, match)
			if(ch_1 * ch_2 >= 0):
				# decrease for both
				old_val = chems.filter(chems.player_1 == pl_id_1 & chems.player_2 == pl_id_2).select("chemistry")
				new_val = old_val-ch
				chem = chem.where("player_1" != pl_id_1 & "player_2" != pl_id_2)
				new_row  = spark.createDataFrame([(pl_id_1, pl_id_2, new_val)], list(chems.columns))

				old_val = chems.filter(chems.player_1 == pl_id_2 & chems.player_2 == pl_id_1).select("chemistry")
				new_val = old_val-ch
				chem = chem.where("player_1" != pl_id_2 & "player_2" != pl_id_1)
				new_row  = spark.createDataFrame([(pl_id_2, pl_id_1, new_val)], list(chems.columns))

			else:
				# increase for both
				old_val = chems.filter(chems.player_1 == pl_id_1 & chems.player_2 == pl_id_2).select("chemistry")
				new_val = old_val+ch
				chem = chem.where("player_1" != pl_id_1 & "player_2" != pl_id_2)
				new_row  = spark.createDataFrame([(pl_id_1, pl_id_2, new_val)], list(chems.columns))

				old_val = chems.filter(chems.player_1 == pl_id_2 & chems.player_2 == pl_id_1).select("chemistry")
				new_val = old_val+ch
				chem = chem.where("player_1" != pl_id_2 & "player_2" != pl_id_1)
				new_row  = spark.createDataFrame([(pl_id_2, pl_id_1, new_val)], list(chems.columns))

	# calculating chemistries for players of team_1 with themselves
	for i in range(len(team_1_ids)):
		for j in range(i+1, len(team_1_ids)):
			ch_1, ch_2, ch = rat_change(team_1_ids[i], team_1_ids[j])
			if(ch_1 * ch_2 < 0):
				# decrease for both
				old_val = chems.filter(chems.player_1 == team_1_ids[i] & chems.player_2 == team_ids[j]).select("chemistry")
				new_val = old_val-ch
				chem = chem.where("player_1" != team_1_ids[i] & "player_2" != team_ids[j])
				new_row  = spark.createDataFrame([(team_1_ids[i], team_ids[j], new_val)], list(chems.columns))

				old_val = chems.filter(chems.player_1 == team_ids[j] & chems.player_2 == team_1_ids[i]).select("chemistry")
				new_val = old_val-ch
				chem = chem.where("player_1" != team_ids[j] & "player_2" != team_1_ids[i])
				new_row  = spark.createDataFrame([(team_ids[j], team_1_ids[i], new_val)], list(chems.columns))
			else:
				# increase for both
				old_val = chems.filter(chems.player_1 == team_1_ids[i] & chems.player_2 == team_ids[j]).select("chemistry")
				new_val = old_val+ch
				chem = chem.where("player_1" != team_1_ids[i] & "player_2" != team_ids[j])
				new_row  = spark.createDataFrame([(team_1_ids[i], team_ids[j], new_val)], list(chems.columns))

				old_val = chems.filter(chems.player_1 == team_ids[j] & chems.player_2 == team_1_ids[i]).select("chemistry")
				new_val = old_val+ch
				chem = chem.where("player_1" != team_ids[j] & "player_2" != team_1_ids[i])
				new_row  = spark.createDataFrame([(team_ids[j], team_1_ids[i], new_val)], list(chems.columns))

	# calculating chemistries for players of team_2 with themselves
	for i in range(len(team_2_ids)):
		for j in range(i+1, len(team_2_ids)):
			ch_1, ch_2, ch = rat_change(team_2_ids[i], team_2_ids[j])
			if(ch_1 * ch_2 < 0):
				# decrease for both
				old_val = chems.filter(chems.player_1 == team_1_ids[i] & chems.player_2 == team_ids[j]).select("chemistry")
				new_val = old_val-ch
				chem = chem.where("player_1" != team_1_ids[i] & "player_2" != team_ids[j])
				new_row  = spark.createDataFrame([(team_1_ids[i], team_ids[j], new_val)], list(chems.columns))

				old_val = chems.filter(chems.player_1 == team_ids[j] & chems.player_2 == team_1_ids[i]).select("chemistry")
				new_val = old_val-ch
				chem = chem.where("player_1" != team_ids[j] & "player_2" != team_1_ids[i])
				new_row  = spark.createDataFrame([(team_ids[j], team_1_ids[i], new_val)], list(chems.columns))
			else:
				# increase for both
				old_val = chems.filter(chems.player_1 == team_1_ids[i] & chems.player_2 == team_ids[j]).select("chemistry")
				new_val = old_val+ch
				chem = chem.where("player_1" != team_1_ids[i] & "player_2" != team_ids[j])
				new_row  = spark.createDataFrame([(team_1_ids[i], team_ids[j], new_val)], list(chems.columns))

				old_val = chems.filter(chems.player_1 == team_ids[j] & chems.player_2 == team_1_ids[i]).select("chemistry")
				new_val = old_val+ch
				chem = chem.where("player_1" != team_ids[j] & "player_2" != team_1_ids[i])
				new_row  = spark.createDataFrame([(team_ids[j], team_1_ids[i], new_val)], list(chems.columns))

# updates the attributes for each player after each event
def handle_event(event,match):
	global playdels
	req_player_id = int(event['playerId'])
	if req_player_id==0:
		pass
	try:
		dateutc = match['dateutc'].split()[0]

		# Pass
		if(event['eventId']==8):
			np  = select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['normal_pass']
			anp = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'accurate_normal_pass':'sum'}).collect()[0]['sum(accurate_normal_pass)']
			kp  = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'key_pass':'sum'}).collect()[0]['sum(key_pass)']
			akp = select_this_match(match).filter(playdels.player_Id==req_player_id).agg({'accurate_key_pass':'sum'}).collect()[0]['sum(accurate_key_pass)']
			tags = [tags["id"] for tags in event["tags"]]
			if 302 in tags:
				if 1801 in tags:
					akp+=1
				kp+=1
			else:
				if 1801 in tags:
					anp+=1
				np+=1
			playdels = playdels.withColumn("normal_pass",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), np).otherwise(playdels["normal_pass"]))
			playdels = playdels.withColumn("accurate_normal_pass",  when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), anp).otherwise(playdels["accurate_normal_pass"]))
			playdels = playdels.withColumn("key_pass",              when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), kp).otherwise(playdels["key_pass"]))
			playdels = playdels.withColumn("accurate_key_pass",     when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), akp).otherwise(playdels["accurate_key_pass"]))
			# print(select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['normal_pass'])
			# print(select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['accurate_normal_pass'])
			# print(select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['key_pass'])
			# print(select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['accurate_key_pass'])
		
		# Duels
		elif(event['eventId']==1):
			# print("player id : ",req_player_id)
			# print(select_this_match(match).show())
			# print(select_this_match(match).filter(playdels.player_Id==req_player_id).show())
			duel_l=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['duels_lost']
			duel_n=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['duels_neutral']
			duel_w=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['duels_won']
			tags = [tags["id"] for tags in event["tags"]]
			if 701 in tags:
				duel_l+=1
			elif 702 in tags:
				duel_n+=1
			elif 703 in tags:
				duel_w+=1
			playdels = playdels.withColumn("duels_lost",    when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), duel_l).otherwise(playdels["duels_lost"]))
			playdels = playdels.withColumn("duels_neutral", when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), duel_n).otherwise(playdels["duels_neutral"]))
			playdels = playdels.withColumn("duels_won",     when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), duel_w).otherwise(playdels["duels_won"]))
		
		# Free kick
		elif(event['eventId']==3):
			effective_f=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['no_of_effective_freekicks']
			total_f=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['total_no_of_freekicks']
			penalties_scored=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['no_of_penalties_scored']
			goals=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['no_of_goals']

			tags = [tags["id"] for tags in event["tags"]]
			if 1801 in tags:
				effective_f+=1
				total_f+=1
			elif 1802 in tags:
				total_f+=1
			elif 101 in tags:
				effective_f+=1
				total_f+=1
				goals+=1
			playdels = playdels.withColumn("no_of_effective_freekicks",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), effective_f).otherwise(playdels["no_of_effective_freekicks"]))
			playdels = playdels.withColumn("no_of_penalties_scored",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), penalties_scored).otherwise(playdels["no_of_penalties_scored"]))
			playdels = playdels.withColumn("total_no_of_freekicks",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), total_f).otherwise(playdels["total_no_of_freekicks"]))
			playdels = playdels.withColumn("no_of_goals",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), goals).otherwise(playdels["no_of_goals"]))

		# Shot on target
		elif(event['eventId']==10):
			shot_t_g=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['shot_on_target_and_goal']
			shot_t_ng=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['shot_on_target_and_not_goal']
			shots_t=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['total_shots']
			goals=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['no_of_goals']

			tags = [tags["id"] for tags in event["tags"]]
			if 1801 in tags:
				if 101 in tags:
					goals+=1
					shot_t_g+=1
					shots_t+=1
				else:
					shot_t_ng+=1
					shots_t+=1  
			elif 1802 in tags:
				shots_t+=1
			playdels = playdels.withColumn("shot_on_target_and_goal",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), shot_t_g).otherwise(playdels["shot_on_target_and_goal"]))
			playdels = playdels.withColumn("shot_on_target_and_not_goal",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), shot_t_ng).otherwise(playdels["shot_on_target_and_not_goal"]))
			playdels = playdels.withColumn("total_shots",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), shots_t).otherwise(playdels["total_shots"]))
			playdels = playdels.withColumn("no_of_goals",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), goals).otherwise(playdels["no_of_goals"]))
		
		# Foul loss
		elif(event['eventId']==2):
				foul=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['no_of_fouls']
				foul+=1
				playdels = playdels.withColumn("no_of_fouls",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), fouls).otherwise(playdels["no_of_fouls"]))
		
		# Own goal
		elif(event['eventId']==102):
				og=select_this_match(match).filter(playdels.player_Id==req_player_id).collect()[0]['no_of_own_goals']
				og+=1
				playdels = playdels.withColumn("no_of_own_goals",           when((playdels["player_Id"] == req_player_id) & (playdels["match_date"] == dateutc) & (playdels["label"] == match['label']), fouls).otherwise(playdels["no_of_own_goals"]))
	except:
		pass


# driver code
def driver(rdd):
    a = rdd.collect()
    match = json.loads(a[0])
    # initialising the event details for each player involved in the match
    initialise_playdel(match)
    playdels.show(50)

    for event in a[1:]:
        # print("event")
        event = json.loads(event)

        # updating attribute values after each event
        handle_event(event,match)
        print(" ------- Player Details after an event ------- ")
        playdels.show()

    # updating the chemistries after each match
    chems_update(match)
    chems.show()

# getting player profile
player_profile = spark.read.options(header='True').csv("file:///home/ishan/Desktop/BD_PROJ/play.csv")
player_profile = player_profile.withColumn("no_of_fouls",lit(0)).withColumn("no_of_goals",lit(0)).withColumn("no_of_own_goals",lit(0)).withColumn("pass_accuracy",lit(0)).withColumn("shots_on_target",lit(0)).withColumn("no_of_matches_played",lit(0)).withColumn("rating",lit(0.5))
player_profile.printSchema()

# getting chemistries
chems = spark.read.options(header='True').csv("file:///home/ishan/Desktop/BD_PROJ/chems.csv")
chems.printSchema()

# training the regression model
def regression_train(match_date):
    global player_profile
    global lrModel
    match_date=datetime.strptime(match_date,"%Y-%m-%d")

    # calculating the age of players on the match day
    input_df = player_profile.withColumn("birthDate", player_profile["birthDate"].cast(DateType()))
    df_reg=input_df.select("name","birthDate","rating")
    df_reg=df_reg.withColumn("Age", months_between(lit(match_date),col("birthDate"))/12)

    # adding square of the age values for 2nd degree regression
    df_reg=df_reg.withColumn("Age_Sq",(col("Age")*col("Age")))
    df_reg.show()

    # creating the input feature vector
    assembler = VectorAssembler(inputCols=["Age", "Age_Sq"],outputCol="features")
    df = assembler.transform(df_reg)

    # initializing the model architecture
    lr = LinearRegression(featuresCol = "features", labelCol = "rating", maxIter=10, regParam=0.3, elasticNetParam=0.8)
    
    # fitting the model to the data
    lrModel = lr.fit(df)

    # metrics of the model
    print("Coefficients: %s" % str(lrModel.coefficients))
    print("Intercept: %s" % str(lrModel.intercept))

def regression_test(match_date, birth_date):
    global lrModel

    # getting the age of the player on the match date
    birth_date=datetime.strptime(birth_date,"%Y-%m-%d")
    diff = relativedelta(match_date, birth_date)
    diff_years = diff.years + diff.months/12 + diff.days/365
    test_df = spark.createDataFrame([(diff_years, diff_yearsdiff_years)], schema_test)

    # adding feature vector
    assembler = VectorAssembler(inputCols=["Age", "Age_Sq"],outputCol="features")
    test = assembler.transform(test_df)
    
    # getting the predicted value
    pred = lrModel.transform(test)
    return pred['rating']

def clustering():
	global player_profile
	global transformed, cluster_rating
	#dataframe for the players needing clustering,those with less than 5 matches
	input_df =  player_profile
	# input_df=input_df.filter(input_df.number_of_matches<5)

	#inputCols would be the name of the columns to cluster on,change appropriately
	vecAssembler = VectorAssembler(inputCols=["rating"], outputCol="features")  
	new_df=vecAssembler.transform(input_df)

	#model fitting
	kmeans = KMeans(k=5, seed=1)  # 5 clusters here
	model = kmeans.fit(new_df.select('features'))

	#adds prediction column showing which cluster who belongs to, and gives new df
	transformed = model.transform(new_df)
	transformed.show() 
	cluster0=transformed.filter(transformed.prediction==0)
	cluster1=transformed.filter(transformed.prediction==1)
	cluster2=transformed.filter(transformed.prediction==2)
	cluster3=transformed.filter(transformed.prediction==3)
	cluster4=transformed.filter(transformed.prediction==4)
	cluster_rating=[0,0,0,0,0]
	cluster_rating[0]=cluster0.select(avg("rating")).collect()[0][0]
	#cluster0_chemistry=cluster0.select(avg("chemistry")).collect()[0][0]

	cluster_rating[1]=cluster1.select(avg("rating")).collect()[0][0]
	#cluster1_chemistry=cluster0.select(avg("chemistry")).collect()[0][0]

	cluster_rating[2]=cluster2.select(avg("rating")).collect()[0][0]
	#cluster2_chemistry=cluster0.select(avg("chemistry")).collect()[0][0]

	cluster_rating[3]=cluster3.select(avg("rating")).collect()[0][0]
	#cluster3_chemistry=cluster0.select(avg("chemistry")).collect()[0][0]

	cluster_rating[4]=cluster4.select(avg("rating")).collect()[0][0]
	#cluster4_chemistry=cluster0.select(avg("chemistry")).collect()[0][0]

	# print(cluster0_rating,cluster1_rating,cluster2_rating,cluster3_rating,cluster4_rating)

clustering()

# initalizing the spark
ssc = StreamingContext(spark.sparkContext, 5)

# creating the dstream object
dstream = ssc.socketTextStream('localhost', 6100)

# operating on each rdds
dstream.foreachRDD(driver)
ssc.start()
ssc.awaitTermination()


input_df=playdels
chemistry_df=chems

f = open('my_inp_predict.json')
input_q = json.load(f) 

# with open("inp_predict.json") as f: #input csv path
# 	# print(f)
# 	input_q = json.loads(json.dumps(f))

# # Queries

input_q_dict=input_q

if 'req_type' in input_q_dict.keys():

	if input_q_dict['req_type']==1:
		player_info_1={}

		with open('play.csv', 'r') as file: #play.csv path
			reader = csv.reader(file)

			for row in reader:
				temp=[]
				temp.append(row[8])
				temp.append(row[4])
				# print(str(row[0]))
				player_info_1[str(row[0])]=temp

		# print(player_info_1)
		def check_team(team):
			T=[]
			gk=0
			defend=0
			midf=0
			fw=0
			# print(player_info_1)
			for x in team.keys():
				if x=='name':
					continue
				else:
					if player_info_1[team[x]][1]=='GK':
						gk+=1
					elif player_info_1[team[x]][1]=='DF':
						defend+=1
					elif player_info_1[team[x]][1]=='MD':
						midf+=1
					elif player_info_1[team[x]][1]=='FW':
						fw+=1
					T.append(team[x])
			if (gk==1) and (defend>=3) and (midf>=2) and (fw>=1) :
				return T
			else:
				print("Invalid")
				return []

		def team_strength(players):
			regression_train(input_q['date'])
			team_strength=0
			for i in players:
				x_2=0
				for j in players:
					if i==j:
						continue
					else:
						x_2+=float(chemistry_df.filter((chemistry_df.player_1 == i) & (chemistry_df.player_2==j)).collect()[0]['chemistry'])
				x_2=x_2/10
				match_played = player_profile.filter(player_profile.Id==int(player_info_1[i][0])).collect()[0]['no_of_matches_played']
				if(match_played<5):
					cluster=transformed.filter(transformed.Id==int(player_info_1[i][0])).collect()[0]['prediction']
					rate = cluster_rating[cluster]
				else:
					rate = regression_test(input_q['date'],player_profile.filter(player_profile.name==i).collect()[0]['birthDate'])
				# rate=player_profile.filter(player_profile.Id==int(player_info_1[i][0])).collect()[0]['rating']
				x_2=x_2*rate
				team_strength+=x_2
			team_strength=team_strength/11
			return team_strength




		team_1_info=input_q_dict['team1']
		team_1_players=check_team(team_1_info)
		# if team_1_players==[]:
		#     break

		team_2_info=input_q_dict['team2']
		team_2_players=check_team(team_2_info)
		# if team_2_players==[]:
		#     break
		team_1_strength=team_strength(team_1_players)
		team_2_strength=team_strength(team_2_players)

		chance_of_1=(0.5+team_1_strength- ((team_1_strength+team_2_strength)/2) ) * 100
		chance_of_1=int(chance_of_1)

		chance_of_2=100- chance_of_1

		predict_dict=dict()

		team1_predict=dict()
		team2_predict=dict()

		team1_predict['name']=input_q_dict['team1']['name']
		team1_predict['winning_chance']=chance_of_1
		team2_predict['name']=input_q_dict['team2']['name']
		team2_predict['winning_chance']=chance_of_2
		predict_dict['team1']=team1_predict
		predict_dict['team2']=team2_predict

		with open('predict_json_output.json', 'w') as json_file: #predict json output path
			json.dump(predict_dict, json_file,indent=2)

	elif input_q_dict['req_type']==2:
		name_q=input_q_dict['name']
		player_dict=dict()
		
		with open('play.csv', 'r') as file: #play.csv path
			reader = csv.reader(file)
			for row in reader:
				if row[0]==name_q:
					x=row
					break
		
		player_dict['name']=name_q
		player_dict['birthArea']=x[1]
		player_dict['birthDate']=x[2]
		player_dict['foot']=x[3]
		player_dict['role']=x[4]
		player_dict['height']=x[5]
		player_dict['passportArea']=x[6]
		player_dict['weight']=x[7]

		input_df_player=input_df.filter(input_df.player_Id==x[8])

		fouls=0
		goals=0
		own_goals=0

		fouls=input_df_player.select(sum('no_of_fouls')).collect()[0]['sum(no_of_fouls)']
		goals=input_df_player.select(sum('no_of_goals')).collect()[0]['sum(no_of_goals)']
		own_goals=input_df_player.select(sum('no_of_own_goals')).collect()[0]['sum(no_of_own_goals)']
		normal_pass=input_df_player.select(sum('normal_pass')).collect()[0]['sum(normal_pass)']
		accurate_normal_pass=input_df_player.select(sum('accurate_normal_pass')).collect()[0]['sum(accurate_normal_pass)']
		key_pass=input_df_player.select(sum('key_pass')).collect()[0]['sum(key_pass)']
		accurate_key_pass=input_df_player.select(sum('accurate_key_pass')).collect()[0]['sum(accurate_key_pass)']
		shots_on_target_and_goal=input_df_player.select(sum('shot_on_target_and_goal')).collect()[0]['sum(shot_on_target_and_goal)']
		shots_on_target_not_goal=input_df_player.select(sum('shot_on_target_and_not_goal')).collect()[0]['sum(shot_on_target_and_not_goal)']
		shots_total=input_df_player.select(sum('total_shots')).collect()[0]['sum(total_shots)']


		pass_accuracy_per= ( (accurate_normal_pass +  (accurate_key_pass*2)  )/( normal_pass + (key_pass*2) ) ) * 100
		# round(pass_accuracy_per,2)

		shot_accuracy_per= ( ( shots_on_target_and_goal + shots_on_target_not_goal*(0.5) )/ shots_total ) *100
		# round(shot_accuracy_per,2)

		player_dict['percent_pass_accuracy']=pass_accuracy_per
		player_dict['percent_shots_on_target']=shot_accuracy_per
		player_dict['fouls']=fouls
		player_dict['goals']=goals
		with open('out_2.json', 'w') as json_file:    #player json output path
			json.dump(player_dict, json_file)

else:
	date=input_q_dict['date']
	#date=datetime.strptime(date,"%Y-%m-%d")
	
	label=input_q_dict['label']
	label_2=label.split(',')
	team1,team2=label_2[0].split('-')
	score1,score2=label_2[1].split('-')

	with open('teams.csv', 'r') as file:    #teams.csv path
		reader = csv.reader(file)
	
		team_info=dict()
		for row in reader:
			if row[0]==team1:
				team_1_id=row[1]
			elif row[0]==team2:
				team_2_id=row[1]
			team_info[row[1]]=row[0]


	input_df_match=input_df.filter(input_df.match_date==date).filter(input_df.label==label)
	match_dict=dict()
	match_dict['date']=date

	x=input_df_match.collect()
	x=x[0]
	if(score1>score2):
		winner=team1
	elif score2>score1:
		winner=team2
	else:
		winner='NULL'

	match_dict['duration']=x['duration']
	match_dict['winner']=winner
	match_dict['venue']=x['venue']
	match_dict['gameweek']=x['gameweek']
	match_dict['goals']=[]
	match_dict['own_goals']=[]
	match_dict['yellow_cards']=[]
	match_dict['red_cards']=[]

	input_df_match_goals=input_df_match.filter(input_df_match.no_of_goals>0)
	input_df_match_own_goals=input_df_match.filter(input_df_match.no_of_own_goals>0)
	input_df_match_yellow_card=input_df_match.filter(input_df_match.yellow_card>0)
	input_df_match_red_card=input_df_match.filter(input_df_match.red_card>0)
	player_info = {}
	with open('play.csv', 'r') as file: #play.csv path
		reader = csv.reader(file)
		for row in reader:
			player_info[row[8]]=row[0]
			
	for row in input_df_match_goals.collect():
		temp=dict()
		temp['name']=player_info[row['player_Id']]
		temp['team']=team_info[row['team_Id']]
		temp['number_of_goals']=row['no_of_goals']
		match_dict['goals'].append(temp)

	for row in input_df_match_own_goals.collect():
		temp=dict()
		temp['name']=player_info[row['player_Id']]
		temp['team']=team_info[row['team_Id']]
		temp['number_of_goals']=row['no_of_own_goals']
		match_dict['own_goals'].append(temp)

	for row in input_df_match_yellow_card.collect():
		match_dict['yellow_cards'].append(player_info[row['player_Id']])

	for row in input_df_match_red_card.collect():
		match_dict['red_cards'].append(player_info[row['player_Id']])

	with open('match_json_output.json', 'w') as json_file:   #player json output path
		json.dump(match_dict, json_file,indent=2)
