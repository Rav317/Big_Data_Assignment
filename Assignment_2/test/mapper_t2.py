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
<<<<<<< HEAD
	adjsrc = adjlist[1][1:len(adjlist[1])-1].split(", ")
	
	dest = [int(i) for i in adjsrc]
	sor = int(adjlist[0])

=======
	sor = int(adjlist[0])
	dest = []
	x=adjlist[1][1:len(adjlist[1])-1]
	dest=list(map(int,x.split(',')))
	#for i in adjlist[1]:
	#	if("0"<=i<="9"):
	#		dest.append(int(i))
	#adjlist=x.split
>>>>>>> 12c34c7b4e52a04ea5c567d75f8fc5e36a92e180
	length = len(dest)

	for val in dest:
		if(val in vkv):
<<<<<<< HEAD
			print(val, vkv[sor]/length, sep = "\t")
=======
			x = vkv[val]/length 
			print(val, round(x,2), sep = "\t")
>>>>>>> 12c34c7b4e52a04ea5c567d75f8fc5e36a92e180
			vis[val] = 1

# for nodes with no incoming edges
for val in vkv:
	if(not vis[val]):
<<<<<<< HEAD
		print(val, 0, sep = "\t")
=======
		print(val, 0, sep = "\t")
>>>>>>> 12c34c7b4e52a04ea5c567d75f8fc5e36a92e180
