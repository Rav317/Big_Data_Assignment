#!/usr/bin/python3

import sys

inter_kv = {}

# reading in the src node : dest node pairs
for line in sys.stdin:
	line = line.strip()
	line = line.split("\t")
		
	k = line[0]
	v = line[1]
	
	# creating an array of dest nodes
	if(k not in inter_kv):
		inter_kv[k] = [v]
	else:
		inter_kv[k].append(v)

# outputing the src node : dest nodes...  as sorted
for key in inter_kv:
	print(key, end = "\t")
	print(inter_kv[key])