package org.kududb.spark.demo.gamer.cdc

import java.text.SimpleDateFormat
import java.util.Random
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, Executors}

import org.kududb.client.{Operation, PartialRow, KuduClient}
import org.kududb.spark.demo.gamer.aggregates.GamerDataGenerator

object DirectDataMultiThreadedInjector {
  val simpleDateFormat = new SimpleDateFormat("MM,dd,yyyy")
  val random = new Random
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<kuduMaster> <tableName> <numberOfRecords> <numberOfThreads>")
      return
    }

    val kuduMaster = args(0)
    val tableName = args(1)
    val numberOfRecords = args(2).toInt
    val executor = Executors.newFixedThreadPool(args(3).toInt)
    val numberOfGamers = args(4).toInt
    val sleepTime = args(5).toInt

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val leftToRun = new AtomicInteger()

    for (i <- 0 to numberOfRecords) {
      leftToRun.incrementAndGet()
      executor.execute(new ApplyNewRecordRunnable(GamerDataGenerator.makeNewGamerRecord(numberOfGamers),
      kuduClient, tableName, leftToRun))
      println("Summited:" + i)

      Thread.sleep(sleepTime)
    }


    val startTime = System.currentTimeMillis()
    while (!executor.awaitTermination(10000, TimeUnit.SECONDS)) {
      val newTime = System.currentTimeMillis()
      println("> Still Waiting: {Time:" + (newTime - startTime) + ", LeftToRun:" + leftToRun + "}" )
    }


    kuduClient.close()


  }
}
