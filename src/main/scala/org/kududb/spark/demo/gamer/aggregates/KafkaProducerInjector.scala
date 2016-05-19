package org.kududb.spark.demo.gamer.aggregates

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object KafkaProducerInjector {


  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{brokerList} {topic} {#OfRecords} {sleepTimeEvery10Records} {#OfGamers}")
      return
    }

    val brokerList = args(0)
    val topic = args(1)
    val numOfRecords = args(2).toInt
    val sleepTimeEvery10Records = args(3).toInt
    val numOfGamers = args(4).toInt

    val producer = getNewProducer(brokerList)

    for (i <- 0 until numOfRecords) {

      val gamerRecord = GamerDataGenerator.makeNewGamerRecord(numOfGamers)

      val message = new ProducerRecord[String, String](topic, gamerRecord.gamerId.toString,  gamerRecord.toString())

      producer.send(message)

      if (i % 10 == 0) {
        Thread.sleep(sleepTimeEvery10Records)
        print(".")
      }
      if (i % 2000 == 0) {
        println()
        println("Records Sent:" + i)
        println()
      }
    }
  }

  def getNewProducer(brokerList:String): KafkaProducer[String, String] = {
    val kafkaProps = new Properties
    kafkaProps.put("bootstrap.servers", brokerList)
    kafkaProps.put("metadata.broker.list", brokerList)

    // This is mandatory, even though we don't send keys
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("acks", "0")

    // how many times to retry when produce request fails?
    kafkaProps.put("retries", "3")
    kafkaProps.put("linger.ms", "2")
    kafkaProps.put("batch.size", "1000")
    kafkaProps.put("queue.time", "2")

    new KafkaProducer[String, String](kafkaProps)
  }


}
