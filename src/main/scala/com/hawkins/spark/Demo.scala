package com.hawkins.spark

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kevin on 11/7/16.
  */
object Demo {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("demo").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER_2)
    val words = lines.flatMap(_.split(" "))
    val filter = words.filter(filtered)
    val wordCounts = filter.map(_ -> 1).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }


  def filtered: (String) => Boolean = {

    val pojo = new DemoJ();
    pojo.setStr("1")

    val result = Filter.filter(pojo)
    if (result) {
      _.length()>0
//      _.equals("1")
    } else {
      _.length()>0
    }
  }
}
