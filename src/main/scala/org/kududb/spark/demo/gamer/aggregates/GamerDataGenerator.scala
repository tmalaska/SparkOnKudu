package org.kududb.spark.demo.gamer.aggregates

import java.util.{Date, Random}

import org.kududb.spark.demo.gamer.GamerEvent

object GamerDataGenerator {

  val random = new Random()
  val averagePlayerPercentage = 40
  val advancedPlayerPercentage = 80
  val superStarPlayerPercentage = 100
  var date = System.currentTimeMillis()

  def makeNewGamerRecord(numOfGamers:Int): GamerEvent = {
    println("date" + new Date(date))
    date += 60000 * 60 * 6
    val playerSelection = random.nextInt(100)
    if (playerSelection < averagePlayerPercentage) {

      val gamerId = random.nextInt(numOfGamers/100) * 100 + playerSelection

      new GamerEvent(gamerId.toString,
        date,
        1,
        if (random.nextInt(10) > 7) 1 else 0,
        random.nextInt(10),
        random.nextInt(20),
        random.nextInt(1000),
        random.nextInt(2000))
    } else if (playerSelection < advancedPlayerPercentage) {
      val gamerId = random.nextInt(numOfGamers/100) * 100 + playerSelection

      new GamerEvent(gamerId.toString,
        date,
        1,
        if (random.nextInt(10) > 5) 1 else 0,
        random.nextInt(20),
        random.nextInt(18),
        random.nextInt(2000),
        random.nextInt(2000))
    } else {
      val gamerId = random.nextInt(numOfGamers/100) * 100 + playerSelection

      new GamerEvent(gamerId.toString,
        date,
        1,
        if (random.nextInt(10) > 3) 1 else 0,
        random.nextInt(20),
        random.nextInt(10),
        random.nextInt(4000),
        random.nextInt(1500))
    }
  }
}
