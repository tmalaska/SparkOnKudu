package org.kududb.spark.demo.gamer.cdc

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger

import org.kududb.client.{Operation, PartialRow, KuduClient}
import org.kududb.spark.demo.gamer.GamerEvent

class ApplyNewRecordRunnable(val gameEvent: GamerEvent,
                              val kuduClient: KuduClient,
                              val tableName: String,
                              val leftToRun:AtomicInteger) extends Runnable{
  override def run(): Unit = {
    val table = kuduClient.openTable(tableName)
    val session = kuduClient.newSession()
    val simpleDateFormat = new SimpleDateFormat("MM,dd,yyyy")

    val record = gameEvent

    val pr = new PartialRow(table.getSchema)
    pr.addString(0, record.gamerId)
    pr.addString(1, "")
    val scannerRows = kuduClient.newScannerBuilder(table).lowerBound(pr).limit(1).build().nextRows()
    val op:Operation = if (scannerRows.hasNext) {
      println(" >> had next")
      val oldRow = scannerRows.next()

      val oldRecordUpdateOp = table.newInsert()

      val row = oldRecordUpdateOp.getRow
      row.addString("gamer_id", oldRow.getString("gamer_id"))
      row.addString("eff_to", simpleDateFormat.format(record.lastTimePlayed))
      row.addString("eff_from", oldRow.getString("eff_from"))
      row.addLong("last_time_played", oldRow.getLong("last_time_played"))
      row.addInt("games_played", oldRow.getInt("games_played"))
      row.addInt("games_won", oldRow.getInt("games_won"))
      row.addInt("oks", oldRow.getInt("oks"))
      row.addInt("deaths", oldRow.getInt("deaths"))
      row.addInt("damage_given", oldRow.getInt("damage_given"))
      row.addInt("damage_taken", oldRow.getInt("damage_taken"))
      row.addInt("max_oks_in_one_game", oldRow.getInt("max_oks_in_one_game"))
      row.addInt("max_deaths_in_one_game", oldRow.getInt("max_deaths_in_one_game"))

      session.apply(oldRecordUpdateOp)
      table.newUpdate()
    } else {
      table.newInsert()
    }

    val row = op.getRow
    row.addString("gamer_id", record.gamerId)
    row.addString("eff_to", "")
    row.addString("eff_from", simpleDateFormat.format(record.lastTimePlayed))
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

    session.flush()
    leftToRun.decrementAndGet()
    println(" >> finished Submit")
  }
}
