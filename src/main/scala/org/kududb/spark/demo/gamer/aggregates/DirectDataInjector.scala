package org.kududb.spark.demo.gamer.aggregates

import java.util.Random

import org.kududb.client.KuduClient

object DirectDataInjector {

  val random = new Random
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<kuduMaster> <tableName> <numberOfRecords>")
      return
    }

    val kuduMaster = args(0)
    val tableName = args(1)
    val numberOfRecords = args(2).toInt

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val table = kuduClient.openTable(tableName)
    val session = kuduClient.newSession()

    table.newInsert()

    for (i <- 0 to numberOfRecords) {
      val record = GamerDataGenerator.makeNewGamerRecord(100000)
      val op = table.newInsert()

      val row = op.getRow
      row.addString("gamer_id", record.gamerId)
      row.addLong("last_time_played", record.lastTimePlayed)
      row.addInt("games_played", record.gamesPlayed)
      row.addInt("games_won", record.gamesWon)
      row.addInt("oks", record.oks)
      row.addInt("deaths", record.deaths)
      row.addInt("damage_given", record.damageGiven)
      row.addInt("damage_taken", record.damageTaken)
      row.addInt("max_oks_in_one_game", record.maxOksInOneGame)
      row.addInt("max_deaths_in_one_game", record.maxDeathsInOneGame)

      session.apply(op)
    }
    session.flush()

    kuduClient.close()


  }
}
