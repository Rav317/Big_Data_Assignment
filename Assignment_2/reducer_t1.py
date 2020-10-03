#!/usr/bin/python3

import sys

kv = {}

# reads in src node : dest nodes...
for line in sys.stdin:
	line = line.strip()
	line = line.split("\t")

	k = line[0]
	v = line[1].split()

	# adding the dest nodes for a src node
	if(k not in kv):
		kv[k] = v
	else:
		kv[k] += v

vpath = sys.argv[1]
f1 = open(vpath, "w")

for key in sorted(kv.keys()):

	# outputting the dest nodes for each src node
	print(str(key)+"\t"+str(sorted(kv[key])))

	# outputting the new initial page rank value for each node
	f1.write(str(key) + ",1\n")

f1.close()