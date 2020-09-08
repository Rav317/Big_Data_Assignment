#!/bin/usr/python3

import sys

# final key-value pairs
ans1 = 0
ans2 = 0

for inter_kv in sys.stdin:
	inter_kv = inter_kv.strip()
	inter_kv = inter_kv.split("\t")

	# getting the key and value 
	key = inter_kv[0]
	value = int(inter_kv[1])
	
	# aggregating the value for each key
	if(value > 0):
		ans1 += value
	else:
		ans2 += value

print(ans1, -ans2, sep = "\n")

