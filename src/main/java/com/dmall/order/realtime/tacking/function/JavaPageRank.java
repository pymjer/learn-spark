package com.dmall.order.realtime.tacking.function;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author: jiang.huang
 * @create Date: 2016/12/7 Time: 17:10
 */
public class JavaPageRank {

    private static final Pattern SPACES = Pattern.compile("\\s+");


    static void showWarning() {
        String warning = "WARN: This is a naive implementation of PageRank " +
                "and is given as an example! \n" +
                "Please use the PageRank implementation found in " +
                "org.apache.spark.graphx.lib.PageRank for more conventional use.";
        System.err.println(warning);
    }

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) {
        showWarning();

        System.setProperty("hadoop.home.dir", "D:\\Software\\hadoop-2.7.3");
        SparkSession spark = SparkSession.builder().appName("JavaPageRank").master("spark://192.168.184.128:7077").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.addJar("D:\\learn\\java\\learn-spark\\target\\spark.jar");

        // 这个地方的问题始终找不到，怀疑是找的hdfs里面的文件
        JavaRDD<String> lines = spark.read().textFile("D:\\learn\\java\\learn-spark\\src\\main\\resources\\urls.txt").javaRDD();
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(line -> {
            String[] split = SPACES.split(line);
            return new Tuple2(split[0], split[1]);
        }).distinct().groupByKey().cache();

        JavaPairRDD<String, Double> ranks = links.mapValues(ss -> 1.0);

        for (int current = 0; current < 5; current++) {
            ranks = links.join(ranks).values().flatMapToPair(s -> {
                int urlCount = Iterables.size(s._1);
                List<Tuple2<String, Double>> results = new ArrayList<>();
                for (String n : s._1) {
                    results.add(new Tuple2<>(n, s._2() / urlCount));
                }
                return results.iterator();
            }).reduceByKey((s1, s2) -> s1 + s2).mapValues(d -> 0.15 + 0.85 * d);
        }

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
        }

        spark.stop();

    }
}
