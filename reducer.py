import sys

# final key-value pairs
kv = {}

for inter_kv in sys.stdin:
	inter_kv = inter_kv.strip()
	inter_kv = inter_kv.split("\t")

	# getting the key and value 
	key = inter_kv[0]
	value = int(inter_kv[1])
	
	# aggregating the value for each key
	if(key not in kv):
		kv[key] = value
	else:
		kv[key] += value


for output in kv:
	# Task-1
	if(output[-1] == "1" or output[-1] == "2"):
		print(kv[output])
	
	# Task-2
	else:
		print(output, kv[output], sep = "\t")

