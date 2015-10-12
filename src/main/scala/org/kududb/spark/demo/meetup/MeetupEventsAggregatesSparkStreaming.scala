package org.kududb.spark.demo.meetup

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.kududb.client.Operation
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.KuduContext
import org.kududb.spark.KuduDStreamFunctions.GenericKuduDStreamFunctions

object MeetupEventAggregatesSparkStreaming {

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

    //val sparkConf = new SparkConf().setAppName("MeetupEventAggregatesSparkStreaming")
    
    val ssc:StreamingContext = setupStreamingContext(runLocal)
    
    
//    if (runLocal) {
//      println("Running Local")
//      val sparkConfig = new SparkConf()
//      sparkConfig.set("spark.broadcast.compress", "false")
//      sparkConfig.set("spark.shuffle.compress", "false")
//      sparkConfig.set("spark.shuffle.spill.compress", "false")
//      sparkConfig.set("spark.io.compression.codec", "lzf")
//      val sc = new SparkContext("local[4]", "SparkSQL on Kudu", sparkConfig)
//      ssc = new StreamingContext(sc, Seconds(2))
//    } else {
//      println("Running Cluster")
//      ssc = new StreamingContext(sparkConf, Seconds(2))
//    }

    val kuduContext = new KuduContext(ssc.sparkContext, kuduMaster)

    //Get original values from Kudu
    val originalKuduDStream = loadOriginalKuduData(tableName, kuduContext, ssc)

    //Connect to Kafka
    val newKafkaMessageDStream = loadDataFromKafka(topics, brokerList, ssc)

    val currentStateDStream = newKafkaMessageDStream.updateStateByKey[MeetupEvent](
        (a:Seq[String], b:Option[MeetupEvent]) => {
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
            val resultingValue = new MeetupEvent()

            //Add up all the values in this micro batch
            while (it.hasNext) {
              val newPart = it.next()
              //resultingValue += MeetupEventBuilder.build(newPart)
            }

          if (b.isEmpty) {
            resultingValue.isInsert = true
            resultingValue.hasChanged = true
            Some(resultingValue)
          } else {
            val existing = b.get
            //existing += resultingValue
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
        val meetupEventTuple = it.next()

        if (meetupEventTuple._2.hasChanged == true) {
          if (meetupEventTuple._2.isInsert) {
            operation = table.newInsert()
          } else {
            operation = table.newUpdate()
          }

          val row = operation.getRow
          row.addString("event_id", meetupEventTuple._2.eventId.toString)
          row.addInt("member_id", meetupEventTuple._2.memberId)
          row.addInt("rsvp_id", meetupEventTuple._2.rsvpId)
          row.addString("event_name", meetupEventTuple._2.eventName)
          row.addString("event_url", meetupEventTuple._2.eventURL)
          row.addLong("time", meetupEventTuple._2.time)
          row.addInt("guests", meetupEventTuple._2.guests)
          row.addString("facebook_identifier", meetupEventTuple._2.facebookIdentifier)
          row.addString("linkedin_identifier", meetupEventTuple._2.linkedinIdentifier)
          row.addString("twitter_identifier", meetupEventTuple._2.twitterIdentifier)
          row.addString("photo", meetupEventTuple._2.photo)
          row.addInt("mtime", meetupEventTuple._2.mtime)
          row.addString("response", meetupEventTuple._2.response)
          row.addDouble("lat", meetupEventTuple._2.latitude)
          row.addDouble("long", meetupEventTuple._2.longitude)
          row.addInt("venue_id", meetupEventTuple._2.venueId)
          row.addString("venue_name", meetupEventTuple._2.venueName)
          row.addString("visibility", meetupEventTuple._2.visibility)

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

    def setupStreamingContext(runLocal:Boolean): StreamingContext = {
      if (runLocal) {
          println("Running Local")
          val sparkConfig = new SparkConf()
          sparkConfig.set("spark.broadcast.compress", "false")
          sparkConfig.set("spark.shuffle.compress", "false")
          sparkConfig.set("spark.shuffle.spill.compress", "false")
          sparkConfig.set("spark.io.compression.codec", "lzf")
          val sc = new SparkContext("local[4]", "SparkSQL on Kudu", sparkConfig)
          new StreamingContext(sc, Seconds(2))
      } else {
        println("Running Cluster")
        val sparkConfig = new SparkConf().setAppName("MeetupEventAggregatesSparkStreaming")
        new StreamingContext(sparkConfig, Seconds(2))
      }
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
                           ssc:StreamingContext):RDD[(String, MeetupEvent)] = {
    val kuduOriginalRdd = kuduContext.kuduRDD(tableName,
      "event_id,member_id,rsvp_id,event_name,event_url,TIME,guests,member_name,facebook_identifier,linkedin_identifier,twitter_identifier,photo,mtime,response,lat,lon,venue_id,venue_name,visibility").
      map(r => {
      val row = r._2

      val eventID = row.getString(0)
      val memberID = row.getInt(1)
      val rsvpID = row.getInt(2)
      val eventName = row.getString(3)
      val eventURL = row.getString(4)
      val time = row.getLong(5)
      val guests = row.getInt(6)
      val memberName = row.getString(7)
      val facebookID = row.getString(8)
      val linkedinID = row.getString(9)
      val twitterID = row.getString(10)
      val photo = row.getString(11)
      val mtime = row.getInt(12)
      val response = row.getString(13)
      val latitude = row.getDouble(14)
      val longitude = row.getDouble(15)
      val venueID = row.getInt(16)
      val venueName = row.getString(17)
      val visibility = row.getString(18)
      
      val initialMeetupEvent = new MeetupEvent(eventID,memberID, rsvpID,
                                               eventName, eventURL,
                                               time, guests,
                                               memberName, facebookID, linkedinID, twitterID, photo,
                                               mtime,
                                               response,
                                               latitude, longitude,
                                               venueID, venueName,
                                               visibility);
      (row.getString(0),initialMeetupEvent)
    })

    kuduOriginalRdd
  }
}
