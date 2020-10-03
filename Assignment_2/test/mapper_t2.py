#!/usr/bin/python3

import sys

f = open("v", "r")
vkv = {}
vis = {}

for line in f:
	line = line.strip()
	line = line.split(",")
	vkv[int(line[0])] = float(line[1])
	vis[int(line[0])] = 0

f.close()

for adjlist in sys.stdin:
	adjlist = adjlist.strip()
	adjlist = adjlist.split("\t")
	adjsrc = adjlist[1][1:len(adjlist[1])-1].split(", ")
	
	dest = [int(i) for i in adjsrc]
	sor = int(adjlist[0])
	length = len(dest)

	for val in dest:
		if(val in vkv):
			print(val, vkv[sor]/length, sep = "\t")
			vis[val] = 1

# for nodes with no incoming edges
for val in vkv:
	if(not vis[val]):
		print(val, 0, sep = "\t")
