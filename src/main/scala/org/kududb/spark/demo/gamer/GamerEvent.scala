package org.kududb.spark.demo.gamer



class GamerEvent(var gamerId:String = "",
                 var lastTimePlayed:Long = 0,
                 var gamesPlayed:Int = 1,
                 var gamesWon:Int = 0,
                 var oks:Int = 0,
                 var deaths:Int = 0,
                 var damageGiven:Int = 0,
                 var damageTaken:Int = 0,
                 var isInsert:Boolean = false,
                 var maxOksInOneGame:Int = 0,
                 var maxDeathsInOneGame:Int = 0,
                 var hasChanged:Boolean = false) extends Serializable {

  override def toString():String = {
    gamerId + "," +
      lastTimePlayed + "," +
      gamesPlayed + "," +
      gamesWon + "," +
      oks + "," +
      deaths + "," +
      damageGiven + "," +
      damageTaken + "," +
      isInsert + "," +
      maxOksInOneGame + "," +
      maxDeathsInOneGame
  }

  def += (gamerEvent: GamerEvent): Unit = {
    gamerId = gamerEvent.gamerId
    lastTimePlayed = gamerEvent.lastTimePlayed
    gamesPlayed += gamerEvent.gamesPlayed
    gamesWon += gamerEvent.gamesWon
    oks += gamerEvent.oks
    deaths += gamerEvent.deaths
    damageGiven += gamerEvent.damageGiven
    damageTaken += gamerEvent.damageTaken
    if (oks > maxOksInOneGame) maxOksInOneGame = oks
    if (deaths > maxDeathsInOneGame) maxDeathsInOneGame = deaths
    isInsert = isInsert && gamerEvent.isInsert
  }
}

object GamerEventBuilder extends Serializable  {
  def build(input:String):GamerEvent = {
    val parts = input.split(",")

    if (parts(0).startsWith("14")) println("input:" + input)

    new GamerEvent(parts(0),
      parts(1).toLong,
      parts(2).toInt,
      parts(3).toInt,
      parts(4).toInt,
      parts(5).toInt,
      parts(6).toInt,
      parts(7).toInt,
      parts(8).equals("true"),
      parts(9).toInt,
      parts(10).toInt)
  }
}
