package org.kududb.spark.demo.basic

import java.util
import java.util.Random

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.KuduClient
import org.kududb.{ColumnSchema, Schema, Type}

object BasicExample {
  def main(args: Array[String]): Unit = {

    val kuduMaster = "quickstart.cloudera"

    println(" -- Starting ")
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    try {
      println(" -- ")

      val columnList = new util.ArrayList[ColumnSchema]()
      columnList.add(new ColumnSchemaBuilder("KEY_ID", Type.STRING).key(true).build())
      columnList.add(new ColumnSchemaBuilder("COL_A", Type.STRING).key(false).build())
      columnList.add(new ColumnSchemaBuilder("COL_B", Type.STRING).key(false).build())
      columnList.add(new ColumnSchemaBuilder("COL_C", Type.STRING).key(false).build())
      val schema = new Schema(columnList)

      if (kuduClient.tableExists("foobar")) {
        kuduClient.deleteTable("foobar")
      }
      kuduClient.createTable("foobar", schema)

      val session = kuduClient.newSession()
      val table = kuduClient.openTable("foobar")

      try {
        val random = new Random()
        for (i <- 0 until 10) {
          val insert = table.newInsert()
          val row = insert.getRow()
          row.addString(0, i.toString)
          row.addString(1, "value " + i)
          row.addString(2, "42:" + i)
          row.addString(3, "Cat" + random.nextGaussian())
          session.apply(insert)
        }
        session.flush()
      } finally {
        session.close()
      }

      val tableList = kuduClient.getTablesList.getTablesList
      for (i <- 0 until tableList.size()) {
        println("Table " + i + ":" + tableList.get(i))
      }

      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      val sc = new SparkContext("local[2]", "SparkSQL on Kudu", sparkConfig)

      val sqlContext = new SQLContext(sc)

      val df = sqlContext.load("org.kududb.spark",
        Map("kudu.table" -> "foobar", "kudu.master" -> kuduMaster))

      df.registerTempTable("foobar")

      sqlContext.sql("SELECT * FROM foobar").foreach(r => {
        println("Row: " + r)
      })
    } finally {
      kuduClient.shutdown()
    }
    println("-- finished")
  }
}
