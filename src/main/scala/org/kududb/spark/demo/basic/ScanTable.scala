package org.kududb.spark.demo.basic

import org.kududb.client.KuduClient

/**
 * Created by ted.malaska on 9/3/15.
 */
object ScanTable {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<kuduMaster> <tableName> <limit>")
      return
    }
    val kuduMaster = args(0)
    val tableName = args(1)
    val limit = args(2).toInt

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val table = kuduClient.openTable(tableName)
    println("starting scan")
    val scannerX = kuduClient.newScannerBuilder(table).build()
    while (scannerX.hasMoreRows) {
      val rows = scannerX.nextRows()
      while (rows.hasNext) {
        val row = rows.next()
        println(" - " + row.rowToString())
      }
    }
    println("finished scan")
    kuduClient.shutdown()
  }
}
