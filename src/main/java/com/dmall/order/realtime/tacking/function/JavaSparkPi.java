package com.dmall.order.realtime.tacking.function;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * @author: jiang.huang
 * @create Date: 2016/12/7 Time: 10:54
 */
public class JavaSparkPi {
    public static void main(String[] args) {
        SparkSession javaSparkPi = SparkSession.builder().appName("JavaSparkPi").master("spark://192.168.184.128:7077").getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(javaSparkPi.sparkContext());
        sparkContext.addJar("D:\\learn\\java\\learn-spark\\target\\spark.jar");
        int slices = 6;
        int total = 1000000 * slices;
        List<Integer> init = Lists.newArrayList();
        for (int i = 0 ; i < total; i++ ) {
            init.add(i);
        }
        Integer result = sparkContext.parallelize(init, slices).map(i -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return x * x + y * y > 1 ? 0 : 1;
        }).reduce((i1, i2) -> i1 + i2);

        double pi = result * 4.0 / total;
        System.out.println("Pi is roughly " + pi);
    }
}
