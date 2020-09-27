#!/usr/bin/python3

import sys

inter_kv = {}

for line in sys.stdin:
	line = line.strip()
	line = line.split()

	k = line[0]
	v = line[1]
	
	if(k not in inter_kv):
		inter_kv[k] = [v]
	else:
		inter_kv[k].append(v)

for key in sorted(inter_kv.keys()):
	print(key, end = " ")
	[print(val, end = " ") for val in sorted(inter_kv[key])]
	print("")