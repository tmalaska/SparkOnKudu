package org.kududb.spark.demo.meetup

import java.util

import org.kududb.{Schema, Type, ColumnSchema}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.{PartialRow, CreateTableBuilder, KuduClient}

object CreateKuduTable {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{kuduMaster} {table}")
      return
    }
    createTable(args(0),args(1))
  }
    
    
    
  def createTable(kuduMaster:String, tableName:String) {  

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val columnList = new util.ArrayList[ColumnSchema]()

    columnList.add(new ColumnSchemaBuilder("event_id", Type.STRING).key(true).build())
    columnList.add(new ColumnSchemaBuilder("member_id", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("rsvp_id", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("event_name", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("event_url", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("time", Type.INT64).key(false).build())
    columnList.add(new ColumnSchemaBuilder("guests", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("member_name", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("facebook_identifier", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("linkedin_identifier", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("twitter_identifier", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("photo", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("mtime", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("response", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("lat", Type.DOUBLE).key(false).build())
    columnList.add(new ColumnSchemaBuilder("long", Type.DOUBLE).key(false).build())
    columnList.add(new ColumnSchemaBuilder("venue_id", Type.INT32).key(false).build())
    columnList.add(new ColumnSchemaBuilder("venue_name", Type.STRING).key(false).build())
    columnList.add(new ColumnSchemaBuilder("visibility", Type.STRING).key(false).build())
    val schema = new Schema(columnList)

    if (kuduClient.tableExists(tableName)) {
      println("Deleting Table")
      kuduClient.deleteTable(tableName)
    }
    val createTableBuilder = new CreateTableBuilder
    val splitRow = schema.newPartialRow()
    splitRow.addString("event_id", "")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "1")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "2")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "3")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "4")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "5")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "6")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "7")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "8")
    createTableBuilder.addSplitRow(splitRow)
    splitRow.addString("event_id", "9")
    createTableBuilder.addSplitRow(splitRow)

    println("Creating Table")
    kuduClient.createTable(tableName, schema, createTableBuilder)
    println("Created Table")
    kuduClient.shutdown()
  }
}
