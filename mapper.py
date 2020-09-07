import sys
import ndjson
import datetime
import math

reader = ndjson.reader(sys.stdin)  # ndjson reader for inputting data

# Task-1
if(len(sys.argv) == 2):
	word = sys.argv[1]   #accessing the word argument

	inter_kv = {}
	# iterating over different ndjson objects
	for section in reader:

		# checking validity of the object according to given constraints
		if(not (all(c.isalpha() or c.isspace() for c in section['word']) and\
			len(section['countrycode']) == 2 and 'A'<=section['countrycode'][0]<='Z' and 'A'<=section['countrycode'][1]<='Z' and\
			(section['recognized'] == True or section['recognized'] == False) and \
			len(section['key_id']) == 16 and all('0'<=c<='9' for c in section['key_id']) and \
			len(section['drawing']) >= 1 and all(len(arr) == 2 for arr in section['drawing']))):
				continue

		# finding the weekday using the timestamp
		yr = int(section['timestamp'][:4])
		mnth = int(section['timestamp'][5:7])
		dy = int(section['timestamp'][8:10])
		dt = datetime.datetime(yr, mnth, dy)
		weekday = dt.weekday()

		# case 1
		if(section['word'] == word and section['recognized'] == True):
			key = section['word']+"1"
			value = 1
			if(key not in inter_kv):
				inter_kv[key] = 1
			else:
				inter_kv[key] += 1
		# case 2
		elif(section['word'] == word and section['recognized'] == False and (weekday == 5 or weekday == 6)):
			key = section['word']+"2"
			value = -1
			if(key not in inter_kv):
				inter_kv[key] = -1
			else:
				inter_kv[key] -= 1

	for kv in inter_kv:
		print(kv, inter_kv[kv], sep = "\t")

# Task-2
elif(len(sys.argv) == 3):

	# accessing the word argument and the distance argument
	word = sys.argv[1]
	k = float(sys.argv[2])

	# combining the result to reduce key-value pairs
	inter_kv = {}

	# iterating over ndjson objects
	for section in reader:

		# checking validity of the objects according to the given constraints
		if(not (all(c.isalpha() or c.isspace() for c in section['word']) and\
			len(section['countrycode']) == 2 and 'A'<=section['countrycode'][0]<='Z' and 'A'<=section['countrycode'][1]<='Z' and\
			(section['recognized'] == True or section['recognized'] == False) and \
			len(section['key_id']) == 16 and all('0'<=c<='9' for c in section['key_id']) and \
			len(section['drawing']) >= 1 and all(len(arr) == 2 for arr in section['drawing']))):
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
				inter_kv[key] = 1
			else:
				inter_kv[key]+=1

	# sorting based on the keys
	for k in sorted(inter_kv.keys()):
		print(k, inter_kv[k], sep = "\t")

	




