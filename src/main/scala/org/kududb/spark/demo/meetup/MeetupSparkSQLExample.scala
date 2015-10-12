package org.kududb.spark.demo.meetup

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object MeetupSparkSQLExample {

  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{kudumaster} {runLocal} {table}")
      return
    }

    Logger.getRootLogger.setLevel(Level.ERROR)

    val kuduMaster = args(0)
    val runLocal = args(1).equals("l")
    var tableName = "meetup"
    if (args.length > 2) {
      tableName = args(2)
    }

    println("Loading Spark Context")
    var sc:SparkContext = null

    if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      sc = new SparkContext("local", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain")
      sc = new SparkContext(sparkConfig)
    }
    println("Loading Spark Context: Finished")

    println("Setting up Tables")
    val sqlContext = new SQLContext(sc)
    sqlContext.load("org.kududb.spark",
      Map("kudu.table" -> tableName, "kudu.master" -> kuduMaster)).registerTempTable(tableName)

    val sqlQuery1 = "SELECT count(*) FROM " + tableName 
    println("Query 1: " + sqlQuery1)
    val startTimeQ1 = System.currentTimeMillis()
    sqlContext.sql(sqlQuery1).take(10).foreach(r => {
      println(" - (" + r + ")")
    })
    println("Finish Query 1: " + (System.currentTimeMillis() - startTimeQ1))

    val sqlQuery2 = "SELECT * FROM " + tableName + " limit 100"
    println("Query 2: " + sqlQuery2)
    val startTimeQ2 = System.currentTimeMillis()
    sqlContext.sql(sqlQuery2).take(100).foreach(r => {
      println(" - (" + r + ")")
    })
    println("Finish Query 2: " + (System.currentTimeMillis() - startTimeQ2))

    
    val sqlQuery3 = "SELECT * FROM " + tableName + " order_by time desc limit 100"
    println("Query 3: " + sqlQuery3)
    val startTimeQ3 = System.currentTimeMillis()
    sqlContext.sql(sqlQuery3).take(100).foreach(r => {
      println(" - (" + r + ")")
    })
    println("Finish Query 3: " + (System.currentTimeMillis() - startTimeQ3))

    
//    println("Query 4: SELECT max(games_played), max(oks), max(damage_given) FROM gamer")
//    val startTimeQ4 = System.currentTimeMillis()
//    sqlContext.sql("SELECT max(games_played), max(oks), max(damage_given) FROM gamer").take(100).foreach(r => {
//      println(" - (" + r + ")")
//    })
//    println("Finish Query 4: " + (System.currentTimeMillis() - startTimeQ4))
//
//    println("Query 5 + MLLIB: SELECT gamer_id, oks, games_won, games_played FROM gamer" )
//    val startTimeQ5 = System.currentTimeMillis()
//    val resultDf = sqlContext.sql("SELECT gamer_id, oks, games_won, games_played FROM gamer")
//
//    val parsedData = resultDf.map(r => {
//      val array = Array(r.getInt(1).toDouble, r.getInt(2).toDouble, r.getInt(3).toDouble)
//      Vectors.dense(array)
//    })
//    val clusters = KMeans.train(parsedData, 3, 5)
//    clusters.clusterCenters.foreach(v => println(" Vector Center:" + v))
//
//    //TODO add Mllib here
//    println("Finish Query 5 + MLLIB: " + (System.currentTimeMillis() - startTimeQ5))

  }
}
