#!/usr/bin/python3

import sys

kv = {}

for line in sys.stdin:
	line = line.strip()
	line = line.split("\t")

	k = int(line[0])
	v = []
	for i in range(0, len(line[1])):
		if(line[1][i] != " "):
			v.append(int(line[1][i]))

	if(k not in kv):
		kv[k] = v
	else:
		kv[k] += v

f1 = open("v", "a")

for key in sorted(kv.keys()):

	print(str(key)+"\t"+str(sorted(kv[key])) + "\n")
	f2.write(str(key) + ", 1\n")

f1.close()