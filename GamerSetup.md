
ssh root@mriggs-strata-1.vpc.cloudera.com

scp -i "tedm2.pem" KuduSpark.jar ec2_user@ec2-52-36-220-83.us-west-2.compute.amazonaws.com:./

--Setting up Kafka
kafka-topics --zookeeper mriggs-strata-1.vpc.cloudera.com:2181 --create --topic gamer --partitions 1 --replication-factor 1
kafka-topics --zookeeper mriggs-strata-1.vpc.cloudera.com:2181 --list
kafka-console-producer --broker-list mriggs-strata-1.vpc.cloudera.com:9092 --topic test
kafka-cocsole-consumer --zookeeper mriggs-strata-1.vpc.cloudera.com:2181 --topic gamer --from-beginning

vi .bash_profile
export PATH=/usr/java/jdk1.7.0_67-cloudera/bin/:$PATH
export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera/

##Populating Kafka
java -cp KuduSpark.jar org.kududb.spark.demo.gamer.KafkaProducerGenerator mriggs-strata-1.vpc.cloudera.com:9092 gamer 10000 300 1000

##create Table
java -cp KuduSpark.jar org.kududb.spark.demo.gamer.CreateKuduTable  ec2-52-36-220-83.us-west-2.compute.amazonaws.com gamer 3

##Run Spark Streaming
spark-submit \
--master yarn --deploy-mode client \
--executor-memory 2G \
--num-executors 2 \
--jars kudu-mapreduce-0.1.0-20150903.033037-21-jar-with-dependencies.jar \
--class org.kududb.spark.demo.gamer.GamerAggergatesSparkStreaming KuduSpark.jar \
mriggs-strata-1.vpc.cloudera.com:9092 gamer mriggs-strata-1.vpc.cloudera.com gamer C

##Run SparkSQL
spark-submit \
--master yarn --deploy-mode client \
--executor-memory 2G \
--num-executors 2 \
--class org.kududb.spark.demo.gamer.GamerSparkSQLExample \
KuduSpark.jar ec2-52-36-220-83.us-west-2.compute.amazonaws.com l

##Run direct insert
java -cp KuduSpark.jar org.kududb.spark.demo.gamer.DirectDataGenerator ec2-52-36-220-83.us-west-2.compute.amazonaws.com gamer 3

##Impala
impala-shell
connect ec2-52-11-171-85.us-west-2.compute.amazonaws.com:21007;

java -cp KuduSpark.jar org.kududb.spark.demo.gamer.cdc.CreateGamerCDCKuduTable  ec2-52-36-220-83.us-west-2.compute.amazonaws.com gamer_cdc 3

java -cp KuduSpark.jar org.kududb.spark.demo.gamer.cdc.DirectDataMultiThreadedInjector  ec2-52-36-220-83.us-west-2.compute.amazonaws.com gamer_cdc 10 5 1000

java -cp KuduSpark.jar org.kududb.spark.demo.gamer.cdc.DirectDataMultiThreadedInjector  ec2-52-36-220-83.us-west-2.compute.amazonaws.com gamer_cdc 100 5 5

java -cp KuduSpark.jar org.kududb.spark.demo.gamer.DropTable ec2-52-36-220-83.us-west-2.compute.amazonaws.com gamer_cdc