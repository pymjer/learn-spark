/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hawkins.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CsvExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL csv data sources example")
      .config("spark.some.config.option", "some-value")
      .master("local[2]")
      .getOrCreate()

    runBasicDataSourceExample(spark)

    spark.stop()
  }

  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    // $example on:generic_load_save_functions$
    val usersDF = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("/Users/hawkins/Downloads/tmdb_5000_movies.csv")

    usersDF.select("budget", "genres").sample(0.1).show();
  }

}
