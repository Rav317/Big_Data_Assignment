# Assignment 1

Assignment is the implementation and application of MapReduce to solve large-scale problems.
This requires Hadoop architecture.
  
### Instructions for installing Hadoop:

`https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation`  

Hadoop version used: 3.2.0  
OS: Ubuntu 20.04  

### The data set used for the assignment can be found here:  

`https://drive.google.com/file/d/1ep_vVC8pvTHTxRjF4dgBLOaxu0aiG2Ku/view?usp=sharing`

<br/><br/>
# Steps to execute MapReduce Program:

1) Cd into hadoop directory  [/home/raghavbd/hadoop]

2) Do `jps` and check jps is running

3) `sbin/start-dfs.sh`

4) `jps`

5) Check NameNode, DataNode, SecondoryNamenode are running  
		**NOTE:** if namenode is not running do `bin/hdfs namenode -format` and start again from step 2

6) `sbin/start-yarn.sh`

7) Do `jps` and Check ResourceManager and NodeManager are running

8) Create an input directory Input in the hdfs : 
			`bin/hdfs dfs -mkdir <input/directory>`
9) Put the input files in the created directory : 
			`bin/hdfs dfs -put <absolute/path/input/file> <input/directory>`

10) Run:

`bin/hadoop jar <streaming/jar/path> \   
-mapper <mapper/file/path> \   
-reducer <reducer/file/path> \   
-input <input/directory>/ \   
-output <output/directory>`   

11) Check that the output directory has a **\_SUCCESS** file which indicates that the execution happened successfully

12) Check the Output directory for the **part** file and cat it to check the output:
			`bin/hdfs dfs -cat <output/directory>/part-0000`

13) Stopping the daemons:
	`sbin/stop-yarn.sh`
	`sbin/stop-dfs.sh`

14) Do `jps` and ensure only jps is running


Example Keys:

<input/directory>  == /Input  
<absolute/path/input/file> == /home/raghavbd/python_wc/inp1  
<streaming/jar/path>  == /home/raghavbd/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar  
<mapper/file/path> == /home/raghavbd/python_wc/mapper.py  
<reducer/file/path> == /home/raghavbd/python_wc/reducer.py  
<output/directory>  == /Output  

`bin/hadoop jar /home/raghavbd/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
-mapper /home/raghavbd/python_wc/mapper.py \
-reducer /home/raghavbd/python_wc/reducer.py \
-input /Input/ \
-output /Output`

