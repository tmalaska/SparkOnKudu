package org.kududb.spark.demo.gamer

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.kududb.client.Operation
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.KuduContext
import org.kududb.spark.KuduDStreamFunctions.GenericKuduDStreamFunctions

object GamerAggergatesSparkStreaming {

  //Logger.getRootLogger.setLevel(Level.ERROR)

  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{brokerList} {topics} {kuduMaster} {tableName} {local}")
    }
    val brokerList = args(0)
    val topics = args(1)
    val kuduMaster = args(2)
    val tableName = args(3)
    val runLocal = args(4).equals("L")

    val sparkConf = new SparkConf().setAppName("GamerAggergatesSparkStreaming")
    var ssc:StreamingContext = null
    if (runLocal) {
      println("Running Local")
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      sparkConfig.set("spark.io.compression.codec", "lzf")
      val sc = new SparkContext("local[4]", "SparkSQL on Kudu", sparkConfig)
      ssc = new StreamingContext(sc, Seconds(2))
    } else {
      println("Running Cluster")
      ssc = new StreamingContext(sparkConf, Seconds(2))
    }

    val kuduContext = new KuduContext(ssc.sparkContext, kuduMaster)

    //Get original values from Kudu
    val originalKuduDStream = loadOriginalKuduData(tableName, kuduContext, ssc)

    //Connect to Kafka
    val newKafkaMessageDStream = loadDataFromKafka(topics, brokerList, ssc)

    val currentStateDStream = newKafkaMessageDStream.updateStateByKey[GamerEvent](
      (a:Seq[String], b:Option[GamerEvent]) => {
        val it = a.iterator
        if (!it.hasNext) {
          if (!b.isEmpty) {
            val existing = b.get
            existing.hasChanged = false
            Some(existing)
          } else {
            None
          }
        } else {
          val resultingValue = new GamerEvent()

          //Add up all the values in this micro batch
          while (it.hasNext) {
            val newPart = it.next()
            resultingValue += GamerEventBuilder.build(newPart)
          }

          if (b.isEmpty) {
            resultingValue.isInsert = true
            resultingValue.hasChanged = true
            Some(resultingValue)
          } else {
            val existing = b.get
            existing += resultingValue
            existing.isInsert = false
            existing.hasChanged = true
            Some(existing)
          }
        }
    }, new HashPartitioner (ssc.sparkContext.defaultParallelism), originalKuduDStream)

    currentStateDStream.kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
      val table = kuduClient.openTable(tableName)

      //This can be made to be faster
      val session = kuduClient.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

      var operation: Operation = null

      var upserts = 0
      while (it.hasNext) {
        val gamerEventTuple = it.next()

        if (gamerEventTuple._2.hasChanged == true) {
          if (gamerEventTuple._2.isInsert) {
            operation = table.newInsert()
          } else {
            operation = table.newUpdate()
          }

          val row = operation.getRow
          row.addString("gamer_id", gamerEventTuple._2.gamerId.toString)
          row.addLong("last_time_played", gamerEventTuple._2.lastTimePlayed)
          row.addInt("games_played", gamerEventTuple._2.gamesPlayed)
          row.addInt("games_won", gamerEventTuple._2.gamesWon)
          row.addInt("oks", gamerEventTuple._2.oks)
          row.addInt("deaths", gamerEventTuple._2.deaths)
          row.addInt("damage_given", gamerEventTuple._2.damageGiven)
          row.addInt("damage_taken", gamerEventTuple._2.damageTaken)
          row.addInt("max_oks_in_one_game", gamerEventTuple._2.maxOksInOneGame)
          row.addInt("max_deaths_in_one_game", gamerEventTuple._2.maxDeathsInOneGame)

          session.apply(operation)

          upserts += 1
        }
      }
      session.close()

      println("upserts: " + upserts)
    })
    ssc.checkpoint("./checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

  def loadDataFromKafka(topics:String,
                        brokerList:String,
                        ssc:StreamingContext): DStream[(String, String)] = {
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.map(r => {
      (r._1, r._2)
    })
  }

  def loadOriginalKuduData(tableName:String,
                           kuduContext:KuduContext,
                           ssc:StreamingContext):RDD[(String, GamerEvent)] = {
    val kuduOriginalRdd = kuduContext.kuduRDD(tableName,
      "gamer_id,last_time_played,games_played,games_won,oks,deaths,damage_given,damage_taken,max_oks_in_one_game,max_deaths_in_one_game").
      map(r => {
      val row = r._2

      val gamerId = row.getString(0)
      val lastTimePlayed = row.getLong(1)
      val gamesPlayed = row.getInt(2)
      val gamesWon = row.getInt(3)
      val oks = row.getInt(4)
      val deaths = row.getInt(5)
      val damageGiven = row.getInt(6)
      val damageTaken = row.getInt(7)
      val maxOksInOneGame = row.getInt(8)
      val maxDeathsInOneGame = row.getInt(9)

      val initialGamerEvent = new GamerEvent(gamerId,lastTimePlayed,
        gamesPlayed,
        gamesWon,
        oks,
        deaths,
        damageGiven,
        damageTaken,
        false,
        maxOksInOneGame,
        maxDeathsInOneGame)

      (row.getString(0),initialGamerEvent)
    })

    kuduOriginalRdd
  }
}
