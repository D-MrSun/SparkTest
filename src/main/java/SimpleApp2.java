/**
 * FileName: SimpleApp2
 * Author:   SAMSUNG-PC 孙中军
 * Date:     2018/11/13 13:58
 * Description: 使用Spark1.6.0 风格测试
 */

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp2 {
    public static void main(String[] args) {
        String logFile = "file:///home/lyq_test/szj/simpleData.txt";
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("================================================>Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}