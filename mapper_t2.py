#!/usr/bin/python3

import sys
import json
import datetime
import math

# function to check vallidity of the json object
def is_valid(section):
	if(all(c.isalpha() or c.isspace() for c in section['word']) and\
			len(section['countrycode']) == 2 and 'A'<=section['countrycode'][0]<='Z' and 'A'<=section['countrycode'][1]<='Z' and\
			(section['recognized'] == True or section['recognized'] == False) and \
			len(section['key_id']) == 16 and all('0'<=c<='9' for c in section['key_id']) and \
			len(section['drawing']) >= 1 and all(len(arr) == 2 and len(arr[0]) == len(arr[1]) and all(type(val) == int for val in arr[0]) and all(type(val) == int for val in arr[1]) for arr in section['drawing'])):
			return True
	
	return False

# accessing the word argument and the distance argument
word = sys.argv[1]
k = float(sys.argv[2])

# combining the result to reduce key-value pairs
inter_kv = {}

# inputting the data from the file
data = []
for line in sys.stdin:
	data.append(json.loads(line))

# iterating over json objects
for section in data:

	# checking validity of the objects according to the given constraints
	if(not is_valid(section)):
		continue

	# getting the x and y coordinates of the 0th coordinate of the first stroke
	xcoor = section['drawing'][0][0][0]
	ycoor = section['drawing'][0][1][0]

	# distance calculation from the origin(0,0)
	dist = math.sqrt(xcoor*xcoor + ycoor*ycoor)

	# if distance is greater than k, output the intermediate key-value pair
	if(dist > k):
		key = section['countrycode']
		value = 1
		if(key not in inter_kv):
			inter_kv[key] = value
		else:
			inter_kv[key] += value

# sorting based on the keys
for k in sorted(inter_kv.keys()):
	print(k, inter_kv[k], sep = "\t")
