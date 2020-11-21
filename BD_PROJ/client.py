# from socket import *
# import json
# import sys
# serverName = 'localhost'
# serverPort = 6100

# #serverName = input("Enter server IP: ")
# #serverPort = int(input("Enter port number: "))

# clientSocket = socket(AF_INET, SOCK_STREAM)
# clientSocket.connect((serverName,serverPort))

# while(1):
# 	json_string = clientSocket.recv(socket.CMSG_LEN).decode()
# 	#print(type(json_string))
# 	#modifiedSentence = json.loads(json_string)
# 	print('From Server:', json_string)
# 	print("\n\n\n\n")
# clientSocket.close()



from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import json

configuration = SparkConf()
configuration.setAppName('Project')

spark_context = SparkContext(conf = configuration)
streaming_context = StreamingContext(spark_context,2)

streaming_context.checkpoint('Project Checkpoint')
input_stream = streaming_context.socketTextStream('localhost',6100)


print(input_stream)

streaming_context.start()

streaming_context.awaitTermination()

streaming_context.stop()