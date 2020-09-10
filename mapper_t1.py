#!/usr/bin/python3

import sys
import ndjson
import datetime


# function to check vallidity of the json object
def is_valid(section):
	if(all(c.isalpha() or c.isspace() for c in section['word']) and\
			len(section['countrycode']) == 2 and 'A'<=section['countrycode'][0]<='Z' and 'A'<=section['countrycode'][1]<='Z' and\
			(section['recognized'] == True or section['recognized'] == False) and \
			len(section['key_id']) == 16 and all('0'<=c<='9' for c in section['key_id']) and \
			len(section['drawing']) >= 1 and all(len(arr) == 2 and len(arr[0]) == len(arr[1]) and all(type(val) == int for val in arr[0]) and all(type(val) == int for val in arr[1]) for arr in section['drawing'])):
			return True
	
	return False

reader = ndjson.reader(sys.stdin)  # ndjson reader for inputting data

word = sys.argv[1]   #accessing the word argument

inter_kv = {}
# iterating over different ndjson objects
for section in reader:

	# checking validity of the object according to given constraints
	if(not is_valid(section)):
		continue

	# finding the weekday using the timestamp
	yr = int(section['timestamp'][:4])
	mnth = int(section['timestamp'][5:7])
	dy = int(section['timestamp'][8:10])
	dt = datetime.datetime(yr, mnth, dy)
	weekday = dt.weekday()

	# case 1
	if(section['word'] == word and section['recognized'] == True):
		key = section['word']+"1"  # adding 1 to symbolize case 1
		value = 1
		if(key not in inter_kv):
			inter_kv[key] = value
		else:
			inter_kv[key] += value
	# case 2
	elif(section['word'] == word and section['recognized'] == False and (weekday == 5 or weekday == 6)):
		key = section['word']+"2"  # adding 2 to symbolize case 2
		value = -1
		if(key not in inter_kv):
			inter_kv[key] = value
		else:
			inter_kv[key] += value

for kv in inter_kv:
	print(kv, inter_kv[kv], sep = "\t")






