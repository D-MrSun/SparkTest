/**
 * FileName: SimpleApp
 * Author:   SAMSUNG-PC 孙中军
 * Date:     2018/11/13 11:54
 * Description:spark2.3.0 简单的测试程序，有多少行出现 a b
 */

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
    public static void main(String[] args) {
        System.out.println("================================================>start spark");
        String logFile = "file:///home/lyq_test/szj/simpleData.txt"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();
        System.out.println("================================================>lamda start");
        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

        System.out.println("================================================>Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}