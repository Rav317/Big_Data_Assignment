#!/usr/bin/python3

import sys

kv = {}

for line in sys.stdin:
	line = line.strip()
	line = line.split()

	k = line[0]
	v = []
	for i in range(1, len(line)):
		v.append(line[i])

	if(k not in kv):
		kv[k] = v
	else:
		kv[k] += v

f1 = open("adj_list", "a")
f2 = open("v", "a")

for key in sorted(kv.keys()):
	for i in range(len(kv[key])):
		kv[key][i] = int(kv[key][i])

	f1.write(str(key)+" "+str(sorted(kv[key])) + "\n")
	f2.write(str(key) + ", 1\n")

f1.close()
f2.close()