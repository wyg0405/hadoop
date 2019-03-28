package com.wyg.spark.day1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Description:jdk8,使用lamda
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-22 16:26
 * @since: JDK1.8
 * @version V1.0
 */
public class WordCountJava8 {

  public static void main(String[] args) {
    /** 获取编程入口 */
    SparkConf conf = new SparkConf();
    conf.setAppName("WordCountJava8").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    /** 加载数据，抽象为RDD */
    String inputFile = "C:\\Users\\wyg04\\Desktop\\word.txt";
    JavaRDD<String> textFile = sc.textFile(inputFile);

    JavaRDD<String> flatMap = textFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
    JavaPairRDD<String, Integer> pairRDD = flatMap.mapToPair(word -> new Tuple2<>(word, 1));
    JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey((x, y) -> x + y);
    JavaPairRDD<String, Integer> sortByKey = reduceByKey.sortByKey();
    sortByKey.foreach(word -> System.out.println(word));
    JavaPairRDD<String, Integer> reduceByValue =
        reduceByKey
            .mapToPair(x -> new Tuple2<>(x._2, x._1))
            .sortByKey(false)
            .mapToPair(x -> new Tuple2<>(x._2, x._1));
    reduceByValue.foreach(x -> System.out.println(x));
    sc.stop();
  }
}
