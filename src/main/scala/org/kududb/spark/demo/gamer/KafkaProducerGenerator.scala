package org.kududb.spark.demo.gamer

import java.util.{Random, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object KafkaProducerGenerator {

  val random = new Random()
  val averagePlayerPercentage = 40
  val advancedPlayerPercentage = 80
  val superStarPlayerPercentage = 100

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

      val gamerRecord = makeNewGamerRecord(numOfGamers)

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

  def makeNewGamerRecord(numOfGamers:Int): GamerEvent = {
    val playerSelection = random.nextInt(100)
    if (playerSelection < averagePlayerPercentage) {

      val gamerId = random.nextInt(numOfGamers/100) * 100 + playerSelection

      new GamerEvent(gamerId.toString,
        System.currentTimeMillis(),
        1,
        if (random.nextInt(10) > 7) 1 else 0,
        random.nextInt(10),
        random.nextInt(20),
        random.nextInt(1000),
        random.nextInt(2000))
    } else if (playerSelection < advancedPlayerPercentage) {
      val gamerId = random.nextInt(numOfGamers/100) * 100 + playerSelection

      new GamerEvent(gamerId.toString,
        System.currentTimeMillis(),
        1,
        if (random.nextInt(10) > 5) 1 else 0,
        random.nextInt(20),
        random.nextInt(18),
        random.nextInt(2000),
        random.nextInt(2000))
    } else {
      val gamerId = random.nextInt(numOfGamers/100) * 100 + playerSelection

      new GamerEvent(gamerId.toString,
        System.currentTimeMillis(),
        1,
        if (random.nextInt(10) > 3) 1 else 0,
        random.nextInt(20),
        random.nextInt(10),
        random.nextInt(4000),
        random.nextInt(1500))
    }
  }
}
