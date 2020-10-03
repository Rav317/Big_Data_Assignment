#!/usr/bin/python3

import sys

kv = {}

# reading the page rank contribution by each node for each node
for line in sys.stdin:
	line = line.strip()
	line = line.split("\t")
	k = line[0]
	v = float(line[1])

	# aggregaing the page rank values for each node
	if(k not in kv):
		kv[k] = v
	else:
		kv[k] += v

# outputting the new page rank values
for k in sorted(kv.keys()):
	print(k,", ", round(0.15 + 0.85*kv[k], 5), sep = "")
