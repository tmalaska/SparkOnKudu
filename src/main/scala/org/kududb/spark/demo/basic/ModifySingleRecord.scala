package org.kududb.spark.demo.basic

import org.kududb.client.{PartialRow, KuduClient}

object ModifySingleRecord {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<kuduMaster> <tableName> <rowKey> <columnIndexToChange> <newValue>")
      return
    }

    val kuduMaster = args(0)
    val tableName = args(1)
    val rowKey = args(2)
    val columnIndexToChange = args(3).toInt
    val newValue = args(4).toInt

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val table = kuduClient.openTable(tableName)
    val session = kuduClient.newSession()

    val lowerBound = new PartialRow(table.getSchema)
    lowerBound.addString(0, rowKey)
    val upperBound = new PartialRow(table.getSchema)
    upperBound.addString(0, rowKey + "_")

    var startTime = System.currentTimeMillis()
    val scanner = kuduClient.newScannerBuilder(table).lowerBound(lowerBound).exclusiveUpperBound(upperBound).build()

    while (scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val row = rows.next()
        println("InitialValue: " + rowKey + " " + row.rowToString())
      }
    }
    println(" scan time spent: " + (System.currentTimeMillis() - startTime))
    scanner.close()

    startTime = System.currentTimeMillis()
    val update = table.newUpdate()
    val row = update.getRow
    row.addString(0, rowKey)
    row.addInt(columnIndexToChange, newValue)
    session.apply(update)
    println("Update: " + rowKey)
    println(" update time spent: " + (System.currentTimeMillis() - startTime))

    startTime = System.currentTimeMillis()
    val scanner2 = kuduClient.newScannerBuilder(table).lowerBound(lowerBound).exclusiveUpperBound(upperBound).build()

    while (scanner2.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val row = rows.next()
        println("NewValue: " + rowKey + " " + row.rowToString())
      }
    }
    scanner2.close()
    println(" scan time spent: " + (System.currentTimeMillis() - startTime))
  }
}
