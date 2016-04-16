hdfs dfs -rm -r /user/srijan/input/*.csv
hdfs dfs -put ratings.csv /user/srijan/input/
hdfs dfs -rm -r /user/srijan/output*
#javac -classpath ../hadoop-core-1.2.1.jar MRNormalize.java
#javac -classpath ../hadoop-core-1.2.1.jar MRDistance.java  
#javac -classpath ../hadoop-core-1.2.1.jar MRContrib.java  
#javac -classpath ../hadoop-core-1.2.1.jar MRAdd.java  
#jar cf mr.jar MR*.class
hadoop jar mr.jar MRNormalize /user/srijan/input/ratings.csv output
hadoop jar mr.jar MRDistance /user/srijan/output/p* output1 20 151000
hadoop fs -get /user/srijan/output1/p* out.csv
hdfs dfs -put out.csv /user/srijan/input/
hadoop jar mr.jar MRContrib /user/srijan/input/*.csv output2 20
hadoop jar mr.jar MRAdd /user/srijan/output2/p* output3
hadoop fs -get /user/srijan/output3/p* result.txt
#hdfs dfs -cat /user/srijan/output3/p*

