#!/usr/bin/python3

import sys

kv = {}

# reads in src node : dest nodes...
for line in sys.stdin:
	line = line.strip("\n")
	line = line.split("\t")

	tlist = line[1][1 : len(line[1])-1].split(", ")
	v = [i[1 : len(i)-1] for i in tlist]
	
	k = line[0]

	# adding the dest nodes for a src node
	if(k not in kv):
		kv[k] = v
	else:
		kv[k] += v

vpath = sys.argv[1]
f1 = open(vpath, "w")

for key in sorted(kv.keys()):

	# outputting the dest nodes for each src node
	print(key+"\t"+str(sorted(kv[key])))

	# outputting the new initial page rank value for each node
	f1.write(key + ",1\n")

f1.close()
f2.close()