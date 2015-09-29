package org.kududb.spark.demo.gamer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object GamerSparkSQLExample {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{kudumaster} {runLocal}")
      return
    }

    Logger.getRootLogger.setLevel(Level.ERROR)

    val kuduMaster = args(0)
    val runLocal = args(1).equals("l")

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
      Map("kudu.table" -> "gamer", "kudu.master" -> kuduMaster)).registerTempTable("gamer")

    println("Query 1: SELECT count(*) FROM gamer")
    val startTimeQ1 = System.currentTimeMillis()
    sqlContext.sql("SELECT count(*) FROM gamer").take(10).foreach(r => {
      println(" - (" + r + ")")
    })
    println("Finish Query 1: " + (System.currentTimeMillis() - startTimeQ1))

    println("Query 2: SELECT * FROM gamer limit 100")
    val startTimeQ2 = System.currentTimeMillis()
    sqlContext.sql("SELECT * FROM gamer limit 100").take(100).foreach(r => {
      println(" - (" + r + ")")
    })
    println("Finish Query 2: " + (System.currentTimeMillis() - startTimeQ2))

    println("Query 3: SELECT * FROM gamer order_by last_time_played desc limit 100")
    val startTimeQ3 = System.currentTimeMillis()
    sqlContext.sql("SELECT * FROM gamer order_by last_time_played desc limit 100").take(100).foreach(r => {
      println(" - (" + r + ")")
    })
    println("Finish Query 3: " + (System.currentTimeMillis() - startTimeQ3))

    println("Query 4: SELECT max(games_played), max(oks), max(damage_given) FROM gamer")
    val startTimeQ4 = System.currentTimeMillis()
    sqlContext.sql("SELECT max(games_played), max(oks), max(damage_given) FROM gamer").take(100).foreach(r => {
      println(" - (" + r + ")")
    })
    println("Finish Query 4: " + (System.currentTimeMillis() - startTimeQ4))

    println("Query 5 + MLLIB: SELECT gamer_id, oks, games_won, games_played FROM gamer" )
    val startTimeQ5 = System.currentTimeMillis()
    val resultDf = sqlContext.sql("SELECT gamer_id, oks, games_won, games_played FROM gamer")

    val parsedData = resultDf.map(r => {
      val array = Array(r.getInt(1).toDouble, r.getInt(2).toDouble, r.getInt(3).toDouble)
      Vectors.dense(array)
    })
    val clusters = KMeans.train(parsedData, 3, 5)
    clusters.clusterCenters.foreach(v => println(" Vector Center:" + v))

    //TODO add Mllib here
    println("Finish Query 5 + MLLIB: " + (System.currentTimeMillis() - startTimeQ5))

  }
}
