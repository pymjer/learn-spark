package com.hawkins.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 测试RDD的生成流程
 *
 * @author hawkins
 * @date 2020/3/20 5:54 下午
 */
object RddTest {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("demo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("./learn-spark/README.md")
    val wc = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wc.collect().foreach(println)

  }

}
