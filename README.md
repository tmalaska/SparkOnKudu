# SparkOnKudu
## Overview
This is a simple reusable lib for working with Kudu with Spark


##Functionality
Current functionality supports the following functions

* RDD foreachPartition with iterator and kudu client
* RDD mapPartition with iterator and kudu client
* DStream foreachPartition with iterator and kudu client
* DStream mapPartition with iterator and kudu client
* Spark SQL integration with Kudu (Basic no filter push down yet)

##Examples
* Basic example
** Creating Kudu tables
** Connecting with SparkSQL
** Converting values from Kudu to SparkSQL to MlLib
* Gamer example
** Creating Kudu Gamer table
** Generating Gamer data and pushing it to Kafka
** Reading Gamer data from Kafka with Spark Streaming
** Aggregating Gamer data in Spark Streaming then pushing mutations to Kudu
** Running Impala SQL on Kudu Gamer table
** Running SparkSQL on Kudu Gamer table
** Converting SparkSQL results to Vectors so we can do KMeans

##Near Future
* Key SQL predict push down
* Need to update POM file with public repo
* Need to work with Kudu project to integrate into Kudu

##Build
mvn clean package

##Setup for Gamer Example

###Setting up Kafka
kafka-topics --zookeeper ZooKeeperNode:2181 --create --topic gamer --partitions 1 --replication-factor 1
kafka-topics --zookeeper ZooKeeperNode:2181 --list

###Basic Testing with Kafka
kafka-console-producer --broker-list BrokerNode:9092 --topic test
kafka-cocsole-consumer --zookeeper ZooKeeperNode:2181 --topic gamer --from-beginning


###Populating Kafka
java -cp KuduSpark.jar org.kududb.spark.demo.gamer.KafkaProducerGenerator mriggs-strata-1.vpc.cloudera.com:9092 gamer 10000 300 1000

###create Table
java -cp KuduSpark.jar org.kududb.spark.demo.gamer.CreateKuduTable  mriggs-strata-1.vpc.cloudera.com gamer

###Run Spark Streaming
spark-submit \
--master yarn --deploy-mode client \
--executor-memory 2G \
--num-executors 2 \
--jars kudu-mapreduce-0.1.0-20150903.033037-21-jar-with-dependencies.jar \
--class org.kududb.spark.demo.gamer.GamerAggergatesSparkStreaming KuduSpark.jar \
mriggs-strata-1.vpc.cloudera.com:9092 gamer mriggs-strata-1.vpc.cloudera.com gamer C







