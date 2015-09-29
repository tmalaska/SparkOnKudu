package org.kududb.spark.demo.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object BasicSparkSQLExamples {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<kuduMaster> <tablename> <runLocal>")
    }

    Logger.getRootLogger.setLevel(Level.ERROR)

    val kuduMaster = args(0)
    val tableName = args(1)
    val runLocal = args(2).equals("l")

    println("starting")
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

    try {
      println("Setting up Tables")
      val sqlContext = new SQLContext(sc)
      sqlContext.load("org.kududb.spark",
        Map("kudu.table" -> tableName, "kudu.master" -> kuduMaster)).registerTempTable(tableName)

      println("Query 1: SELECT count(*) FROM " + tableName)
      val startTimeQ1 = System.currentTimeMillis()
      sqlContext.sql("SELECT count(*) FROM " + tableName).take(10).foreach(r => {
        println(" - (" + r + ")")
      })
      println("Finish Query 1: " + (System.currentTimeMillis() - startTimeQ1))

      println("Query 2: SELECT key_id, col_1 FROM " + tableName + " limit 100")
      val startTimeQ2 = System.currentTimeMillis()
      sqlContext.sql("SELECT key_id, col_1 FROM " + tableName + " limit 100 ").take(100).foreach(r => {
        println(" - (" + r + ")")
      })
      println("Finish Query 2: " + (System.currentTimeMillis() - startTimeQ2))

      val q3 = "select key_id from " + tableName + " a join (SELECT max(col_1) col_max FROM " + tableName + ") b on (a.col_1 = b.col_max)"
      println("Query 3: " + q3)
      val startTimeQ3 = System.currentTimeMillis()
      sqlContext.sql(q3).take(100).foreach(r => {
        println(" - (" + r + ")")
      })
      println("Finish Query 3: " + (System.currentTimeMillis() - startTimeQ3))
/*
      val q4 = "select host, metric, avg(value), count(*) from metrics group by host, metric"
      println("Query 4: " + q4)
      val startTimeQ4 = System.currentTimeMillis()
      sqlContext.sql(q4).take(100).foreach(r => {
        println(" - (" + r + ")")
      })
      println("Finish Query 4: " + (System.currentTimeMillis() - startTimeQ4))

*/

      println("Query 5 + MLLIB: SELECT key_id, col_1, col_2 FROM " + tableName )
      val startTimeQ5 = System.currentTimeMillis()
      val resultDf = sqlContext.sql("SELECT key_id, col_1, col_2 FROM " + tableName + " limit 1000")

      val parsedData = resultDf.map(r => {
        val array = Array(r.getInt(1).toDouble, r.getInt(2).toDouble)
        Vectors.dense(array)
      })
      val clusters = KMeans.train(parsedData, 3, 4)
      clusters.clusterCenters.foreach(v => println(" Vector Center:" + v))

      //TODO add Mllib here
      println("Finish Query 5 + MLLIB: " + (System.currentTimeMillis() - startTimeQ5))

    } finally {
      sc.stop()
    }
  }
}
