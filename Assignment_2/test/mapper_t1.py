#!/usr/bin/python3

import sys

inter_kv = {}

for line in sys.stdin:
	line = line.strip()
	line = line.split("\t")

	if(len(line)<2):
		continue
		
	k = line[0]
	v = line[1]
	
	if(k not in inter_kv):
		inter_kv[k] = [v]
	else:
		inter_kv[k].append(v)

for key in sorted(inter_kv.keys()):
	if(not inter_kv[key]):
		continue
	print(key, end = "\t")
	[print(val, end = " ") for val in sorted(inter_kv[key])]
	print("")