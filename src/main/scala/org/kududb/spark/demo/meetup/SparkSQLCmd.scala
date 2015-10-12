package org.kududb.spark.demo.meetup

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object SparkSQLCmd {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{kuduMaster} {L for Local} {table}")
    }

    val kuduMaster = args(0)
    val runLocal = args(1).equals("L")
    var tableName = "meetup";
    if (args.length > 2) {
      tableName = args(2)
    }

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

    val sqlContext = new SQLContext(sc)
    println("Loading '" + tableName + "' table")
    sqlContext.load("org.kududb.spark",
      Map("kudu.table" -> tableName, "kudu.master" -> kuduMaster)).registerTempTable(tableName)
    println("Successfully loaded '" + tableName + "' table")

    val doContinue = true

    while (doContinue) {
      val input = readLine("SparkSQL> ")

      try {


        val startTime = System.currentTimeMillis()
        val startTimeQ1 = System.currentTimeMillis()
        sqlContext.sql(input).take(1000).foreach(r => {
          println(" > " + r)
        })
        println(" Finished in " + (System.currentTimeMillis() - startTime))
      } catch {
        case e: Throwable => {
          println(" > Query '" + input + "' failed.")
          e.printStackTrace()
        }
      }
    }

  }
}
