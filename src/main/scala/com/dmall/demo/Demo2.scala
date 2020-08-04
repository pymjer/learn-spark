package com.dmall.demo

import com.dmall.order.realtime.tacking.function.{DemoJ, Filter}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

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
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER_2)
    val rows = lines.map(_.split(" "))
    val notEmpty = rows.map(_.filter(x => !x.isEmpty())).map((line:Array[String]) => (line.head,line.last))
    val count = notEmpty.map((pair:(String,String)) => (pair._1,1)).reduceByKey(_ + _)
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def statusGroup(x:Map[String,Int],y:Map[String,Int]):Map[String,Int] = {
      var result:Map[String,Int] = Map()


      val yhead: (String, Int) = y.head
      var contains = false

      x.foreach((pair:(String,Int)) => {
        if(yhead._1.equals(pair._1)) {
          result = result + (pair._1 -> (pair._2 + yhead._2))
          contains = true
        } else {
          result = result + pair
        }
      })

      if (!contains) {
        result = result ++ y
        result
      } else {
        result
      }
  }
}
