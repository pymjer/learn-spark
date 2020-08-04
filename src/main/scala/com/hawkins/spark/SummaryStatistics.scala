package com.hawkins.spark

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对于RDD[Vector]类型，Spark MLlib提供了colStats的统计方法，该方法返回一个MultivariateStatisticalSummary的实例。他封装了列的最大值，最小值，均值、方差、总数
  * @author: jiang.huang
  * @create Date: 2016/12/20 Time: 15:03
  */
object SummaryStatistics {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Software\\hadoop-2.7.3")
    val conf = new SparkConf().setAppName("SummaryStatics").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val observations = sc.textFile("D:\\learn\\java\\learn-spark\\src\\main\\resources\\numbers.txt").map(_.split(" "))
      .map(s => {
        val doubleS = new Array[Double](s.length)

        for ( i <- 0 to s.length - 1) {
          doubleS(i) = s(i).toDouble
        }

        doubleS
      }).map(line => Vectors.dense(line))

    val stats: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(stats.mean)// 平均值
    println(stats.variance) //方差
    println(stats.numNonzeros) //

  }
}
