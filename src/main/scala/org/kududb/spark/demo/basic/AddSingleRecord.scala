package org.kududb.spark.demo.basic

import java.util.Random

import org.kududb.client.{PartialRow, KuduClient}

object AddSingleRecord {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<kuduMaster> <tableName> <rowKey>")
      return
    }

    val kuduMaster = args(0)
    val tableName = args(1)
    val rowKey = args(2)

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val table = kuduClient.openTable(tableName)
    val session = kuduClient.newSession()

    val lowerBound = new PartialRow(table.getSchema)
    lowerBound.addString(0, rowKey)
    val upperBound = new PartialRow(table.getSchema)
    upperBound.addString(0, rowKey + "_")

    var startTime = System.currentTimeMillis()
    val random = new Random()

    startTime = System.currentTimeMillis()
    val update = table.newInsert()
    val row = update.getRow
    row.addString(0, rowKey)
    val columns = table.getSchema.getColumns
    for (c <- 1 until columns.size()) {
      println(columns.get(c).getName + " " + columns.get(c).getType)
      row.addInt(columns.get(c).getName, random.nextInt(100000))
    }
    session.apply(update)
    println("new key: " + rowKey)
    println(" new key time spent: " + (System.currentTimeMillis() - startTime))

    startTime = System.currentTimeMillis()
    val scanner2 = kuduClient.newScannerBuilder(table).lowerBound(lowerBound).exclusiveUpperBound(upperBound).build()

    while (scanner2.hasMoreRows) {
      val rows = scanner2.nextRows()
      while (rows.hasNext) {
        val row = rows.next()
        println("NewValue: " + rowKey + " " + row.rowToString())
      }
    }
    scanner2.close()
    println(" scan time spent: " + (System.currentTimeMillis() - startTime))

    val scannerX = kuduClient.newScannerBuilder(table).build()
    while (scannerX.hasMoreRows) {
      val rows = scannerX.nextRows()
      while (rows.hasNext) {
        val row = rows.next()
        println("Full Scan: " + row.rowToString())
      }
    }
    println("done")
    kuduClient.shutdown()
  }
}
