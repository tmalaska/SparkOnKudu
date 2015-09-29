package org.kududb.spark.demo.basic

import java.util
import java.util.Random

import org.kududb.{Schema, Type, ColumnSchema}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.{AsyncKuduClient, KuduClient}


object InitialDataPopulation {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<kuduMaster> <TableName> <numberOfColumns> <numberOfRows>")

      //"quickstart.cloudera"

      return
    }
    val kuduMaster = args(0)
    val tableName = args(1)
    val numOfColumns = args(2).toInt
    val numOfRows = args(3).toInt

    val kuduClient = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster).build()
    try {
      //Delete table if exist
      if (kuduClient.tableExists(tableName).join()) {
        kuduClient.deleteTable(tableName).join()
      }

      //Create Schema
      val columnList = new util.ArrayList[ColumnSchema]()
      columnList.add(new ColumnSchemaBuilder("key_id", Type.STRING).key(true).build())
      for (c <- 0 until numOfColumns) {
        columnList.add(new ColumnSchemaBuilder("col_" + c, Type.INT32).key(false).build())
      }
      val schema = new Schema(columnList)

      //Create table
      kuduClient.createTable(tableName, schema).join()

      //Populate table
      val random = new Random
      val table = kuduClient.openTable(tableName).join()
      val asyncSession = kuduClient.newSession()

      for (r <- 0 until numOfRows) {
        val insert = table.newInsert()
        val row = insert.getRow()
        row.addString(0, NameGenerator.getName())
        val columns = table.getSchema.getColumns
        for (c <- 1 until columns.size()) {
          row.addInt(columns.get(c).getName, random.nextInt(100000))
        }
        asyncSession.apply(insert)

        if (r % 1000 == 0) {
          println("Inserted: " + r)
        }
      }
      asyncSession.flush()

      val scannerX = kuduClient.newScannerBuilder(table).build()
      while (scannerX.hasMoreRows) {
        val rows = scannerX.nextRows().join()
        while (rows.hasNext) {
          val row = rows.next()
          println(" - " + row.rowToString())
        }
      }

      asyncSession.close()

    } finally {
      kuduClient.shutdown()
    }
  }
}
