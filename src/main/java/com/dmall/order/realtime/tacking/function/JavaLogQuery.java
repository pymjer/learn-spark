package com.dmall.order.realtime.tacking.function;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: jiang.huang
 * @create Date: 2016/12/7 Time: 16:41
 */
public class JavaLogQuery {
    public static final List<String> exampleApacheLogs = Lists.newArrayList(
            "10.10.10.10 - \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg " +
                    "HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                    "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                    ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                    "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" " +
                    "62.24.11.25 images.com 1358492167 - Whatup",
            "10.10.10.10 - \"FRED\" [18/Jan/2013:18:02:37 +1100] \"GET http://images.com/2013/Generic.jpg " +
                    "HTTP/1.1\" 304 306 \"http:/referall.com\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; " +
                    "GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR " +
                    "3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR  " +
                    "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.352 \"-\" - \"\" 256 977 988 \"\" " +
                    "0 73.23.2.15 images.com 1358492557 - Whatup");

    public static final Pattern apacheLogRegex = Pattern.compile(
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");


    /** Tracks the total query count and number of aggregate bytes for a particular group. */
    public static class Stats implements Serializable {

        private final int count;
        private final int numBytes;

        public Stats(int count, int numBytes) {
            this.count = count;
            this.numBytes = numBytes;
        }
        public Stats merge(Stats other) {
            return new Stats(count + other.count, numBytes + other.numBytes);
        }

        public String toString() {
            return String.format("bytes=%s\tn=%s", numBytes, count);
        }
    }

    public static Tuple3<String,String,String> extractKey(String line) {
        Matcher matcher = apacheLogRegex.matcher(line);
        if (matcher.find()) {
            String ip = matcher.group(1);
            String user = matcher.group(3);
            String query = matcher.group(5);
            if (!user.equalsIgnoreCase("-")) {
                return new Tuple3<>(ip,user,query);
            }
        }
        return new Tuple3<>(null,null,null);
    }

    public static Stats extractStats(String line) {
        Matcher matcher = apacheLogRegex.matcher(line);
        if(matcher.find()) {
            int bytes = Integer.parseInt(matcher.group(7));
            return new Stats(1,bytes);
        }
        return new Stats(1,0);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("JavaLogQuery").master("spark://192.168.184.128:7077").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.addJar("D:\\learn\\java\\learn-spark\\target\\spark.jar");
        JavaPairRDD<Tuple3, Stats> counts = jsc.parallelize(exampleApacheLogs).mapToPair(s -> new Tuple2<Tuple3, Stats>(extractKey(s), extractStats(s))).reduceByKey((s1, s2) -> s1.merge(s2));
        List<Tuple2<Tuple3, Stats>> collect = counts.collect();
        for (Tuple2 t: collect) {
            System.out.println(t._1 + "\t" + t._2);
        }
        spark.stop();
    }
}