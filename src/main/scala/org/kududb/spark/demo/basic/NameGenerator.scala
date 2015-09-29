package org.kududb.spark.demo.basic

import java.util.Random

import scala.collection.mutable

object NameGenerator {

  val random = new Random()
  val listOfNames = new mutable.MutableList[NameAndCounter]
  listOfNames += new NameAndCounter("Katlyn")
  listOfNames += new NameAndCounter("Laurena")
  listOfNames += new NameAndCounter("Jenise")
  listOfNames += new NameAndCounter("Vida")
  listOfNames += new NameAndCounter("Delphine")
  listOfNames += new NameAndCounter("Tiffanie")
  listOfNames += new NameAndCounter("Carroll")
  listOfNames += new NameAndCounter("Steve")
  listOfNames += new NameAndCounter("Nu")
  listOfNames += new NameAndCounter("Robbin")
  listOfNames += new NameAndCounter("Mahalia")
  listOfNames += new NameAndCounter("Norah")
  listOfNames += new NameAndCounter("Selina")
  listOfNames += new NameAndCounter("Cornelius")
  listOfNames += new NameAndCounter("Bennie")
  listOfNames += new NameAndCounter("Kemberly")
  listOfNames += new NameAndCounter("Johnie")
  listOfNames += new NameAndCounter("Jenee")
  listOfNames += new NameAndCounter("Napoleon")
  listOfNames += new NameAndCounter("Brenton")
  listOfNames += new NameAndCounter("Roxana")
  listOfNames += new NameAndCounter("Kalyn")
  listOfNames += new NameAndCounter("Jeana")
  listOfNames += new NameAndCounter("Tennie")
  listOfNames += new NameAndCounter("Tasia")
  listOfNames += new NameAndCounter("Ashely")
  listOfNames += new NameAndCounter("Hester")
  listOfNames += new NameAndCounter("Zita")
  listOfNames += new NameAndCounter("Evalyn")
  listOfNames += new NameAndCounter("Anderson")
  listOfNames += new NameAndCounter("Elaina")
  listOfNames += new NameAndCounter("Benny")
  listOfNames += new NameAndCounter("Heidi")
  listOfNames += new NameAndCounter("Mammie")
  listOfNames += new NameAndCounter("Alisa")
  listOfNames += new NameAndCounter("Billie")
  listOfNames += new NameAndCounter("Wan")
  listOfNames += new NameAndCounter("Dionna")
  listOfNames += new NameAndCounter("Julene")
  listOfNames += new NameAndCounter("Chasidy")
  listOfNames += new NameAndCounter("Vennie")
  listOfNames += new NameAndCounter("Cara")
  listOfNames += new NameAndCounter("Charissa")
  listOfNames += new NameAndCounter("Russell")
  listOfNames += new NameAndCounter("Daniela")
  listOfNames += new NameAndCounter("Kindra")
  listOfNames += new NameAndCounter("Eduardo")
  listOfNames += new NameAndCounter("Marci")
  listOfNames += new NameAndCounter("Gustavo")
  listOfNames += new NameAndCounter("Dianna	")

  def getName(): String = {
    val nameAndCounter = listOfNames.get(random.nextInt(listOfNames.length - 1)).get
    nameAndCounter.counter += 1
    nameAndCounter.name + "_" + nameAndCounter.counter
  }
}

class NameAndCounter(val name:String = "N/A", var counter:Int = 0) {

}
