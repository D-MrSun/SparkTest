import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * FileName: SortWordCount
 * Author:   SAMSUNG-PC 孙中军
 * Date:     2018/11/13 14:06
 * Description: 对出现的单词进行统计排序
 * 1.创建RDD
 * 2.将文本进行拆分 （flatMap)
 * 3.将拆分后的单词进行统计 (mapToPair,reduceByKey)
 * 4.反转键值对 (mapToPair)
 * 5.按键升序排序 (sortedByKey)
 * 6.再次反转键值对  (mapToPair)
 * 7.打印输出(foreach)
 */

public class SortWordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("SortWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 创建lines RDD
        JavaRDD<String> lines = sc.textFile("file:///home/lyq_test/szj/simpleData.txt");
        // 将文本分割成单词RDD
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        //将单词RDD转换为（单词，1）键值对RDD
        JavaPairRDD<String,Integer> wordPair = words.mapToPair(new PairFunction<String, String,Integer>() {
            @Override
            public Tuple2<String,Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });
        //对wordPair 进行按键计数
        JavaPairRDD<String,Integer> wordCount = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer +integer2;
            }
        });
        // 到这里为止，就得到了每个单词出现的次数
        // 我们的新需求，是要按照每个单词出现次数的顺序，降序排序
        // wordCounts RDD内的元素是这种格式：(spark, 3) (hadoop, 2)
        // 因此我们需要将RDD转换成(3, spark) (2, hadoop)的这种格式，才能根据单词出现次数进行排序

        // 进行key-value的反转映射
        JavaPairRDD<Integer,String> countWord = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> s) throws Exception {
                return new Tuple2<Integer, String>(s._2,s._1);
            }
        });
        // 按照key进行排序
        JavaPairRDD<Integer, String> sortedCountWords = countWord.sortByKey(false);
        // 再次将value-key进行反转映射
        JavaPairRDD<String,Integer> sortedWordCount = sortedCountWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
                return new Tuple2<String, Integer>(s._2,s._1);
            }
        });
        // 到此为止，我们获得了按照单词出现次数排序后的单词计数
        // 打印出来
        sortedWordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println("===========================>word \""+s._1+"\" appears "+ s._2+" times.");
            }
        });
        sc.close();
    }
}