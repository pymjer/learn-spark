package com.dmall.demo

import com.dmall.order.realtime.tacking.function.{DemoJ, Filter}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kevin on 11/7/16.
  */
object Demo2 {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("demo").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER_2)
    val rows = lines.map(_.split(" "))
    val notEmpty = rows.map(_.filter(x => !x.isEmpty()))
    notEmpty.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
