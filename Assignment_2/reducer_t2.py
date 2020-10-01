#!/usr/bin/python3

import sys

kv = {}

for line in sys.stdin:
	line = line.strip()
	line = line.split("\t")
	k = int(line[0])
	v = float(line[1])

	if(k not in kv):
		kv[k] = v
	else:
		kv[k] += v

for k in sorted(kv.keys()):
	print(k,", ", round(0.15 + 0.85*kv[k], 5), sep = "")
