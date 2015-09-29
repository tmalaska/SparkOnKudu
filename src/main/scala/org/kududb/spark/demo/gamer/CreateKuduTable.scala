package org.kududb.spark.demo.gamer

import java.util

import org.kududb.{Schema, Type, ColumnSchema}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.{PartialRow, CreateTableBuilder, KuduClient}

object CreateKuduTable {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{kuduMaster} {tableName}")
      return
    }

    val kuduMaster = args(0)
    val tableName = args(1)

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val columnList = new util.ArrayList[ColumnSchema]()

    columnList.add(new ColumnSchemaBuilder("gamer_id", Type.STRING).key(true).build())
    columnList.add(new ColumnSchemaBuilder("last_time_played", Type.INT64).key(false).build())
    columnList.add(new ColumnSchemaBuilder("games_played", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("games_won", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("oks", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("deaths", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("damage_given", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("damage_taken", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("max_oks_in_one_game", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("max_deaths_in_one_game", Type.INT32).key(false).build())
    val schema = new Schema(columnList)

    if (kuduClient.tableExists(tableName)) {
      println("Deleting Table")
      kuduClient.deleteTable(tableName)
    }
    val createTableBuilder = new CreateTableBuilder
    val splitRow = schema.newPartialRow()
    splitRow.addString("gamer_id", "")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "1")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "2")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "3")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "4")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "5")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "6")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "7")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "8")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("gamer_id", "9")
    createTableBuilder.addSplitRow(splitRow)

    println("Creating Table")
    kuduClient.createTable(tableName, schema, createTableBuilder)
    println("Created Table")
    kuduClient.shutdown()
  }
}
