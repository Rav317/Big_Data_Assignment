#!/usr/bin/python3

import sys

f = open("v", "r")
vkv = {}

for line in f:
	vkv[int(line[0])] = int(line[3])

f.close()

for adjlist in sys.stdin:
	adjlist = adjlist.strip()
	adjlist = adjlist.split("\t")
	sor = int(adjlist[0])
	dest = []
	for i in adjlist[1]:
		if("0"<=i<="9"):
			dest.append(int(i))
	length = len(dest)

	for val in dest:
		print(val, vkv[val]/length, sep = "\t")