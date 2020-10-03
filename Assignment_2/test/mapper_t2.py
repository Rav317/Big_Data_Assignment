#!/usr/bin/python3

import sys

vkv = {}
vis = {}

f = open("v", "r")

for line in f:
	line = line.strip()
	line = line.split(",")
	vkv[line[0]] = float(line[1])
	vis[line[0]] = False

f.close()

for adjlist in sys.stdin:
	adjlist = adjlist.strip()
	adjlist = adjlist.split("\t")
	adjsrc = adjlist[1][1 : len(adjlist[1])-1].split(", ")
	
	dest = [i[1:len(i)-1] for i in adjsrc]
	sor = adjlist[0]
	length = len(dest)

	for val in dest:
		print(val, vkv[sor]/length, sep = "\t")
		vis[val] = True

# for nodes with no incoming edges
for val in vkv:
	if(not vis[val]):
		print(val, 0, sep = "\t")
