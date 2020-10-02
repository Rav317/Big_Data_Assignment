#!/usr/bin/python3

import sys

f = open("v", "r")
vkv = {}
vis = {}

# reading in the page rank values
for line in f:
	line = line.strip()
	line = line.split(", ")
	vkv[int(line[0])] = float(line[1])
	vis[int(line[0])] = 0

f.close()

# reading the adj list
for adjlist in sys.stdin:
	adjlist = adjlist.strip()
	adjlist = adjlist.split("\t")
	sor = int(adjlist[0])
	dest = []
	for i in adjlist[1]:
		if("0"<=i<="9"):
			dest.append(int(i))
			
	length = len(dest)

	# outputting page rank contribution by each node for a node
	for val in dest:
		if(val in vkv):
			print(val, vkv[val]/length, sep = "\t")
			vis[val] = 1

# page rank for nodes with no incoming edges
for val in vkv:
	if(not vis[val]):
		print(val, 0, sep = "\t")