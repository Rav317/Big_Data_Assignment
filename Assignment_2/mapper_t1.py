#!/usr/bin/python3

import sys

inter_kv = {}

# reading in the src node : dest node pairs
for line in sys.stdin:
	line = line.strip()
	line = line.split("\t")

	if(len(line)<2):
		continue
		
	k = line[0]
	v = line[1]
	
	# creating an array of dest nodes
	if(k not in inter_kv):
		inter_kv[k] = [v]
	else:
		inter_kv[k].append(v)

# outputing the src node : dest nodes...  as sorted
for key in sorted(inter_kv.keys()):
	if(not inter_kv[key]):
		continue
	print(key, end = "\t")
	[print(val, end = " ") for val in sorted(inter_kv[key])]
	print("")