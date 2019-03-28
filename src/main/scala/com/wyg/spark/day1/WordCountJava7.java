package com.wyg.spark.day1;

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
 * java7的spark使用
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-22 14:17
 * @version V1.0
 */
public class WordCountJava7 {
  public static void main(String[] args) {

    /** 获取编程入口 */
    SparkConf conf = new SparkConf();
    conf.setAppName("WordCountJava8").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    /** 加载数据，抽象为RDD */
    String inputFile = "C:\\Users\\wyg04\\Desktop\\word.txt";
    JavaRDD<String> textFile = sc.textFile(inputFile);

    /** 对RDD处理 */
    JavaRDD<String> flatMap =
        textFile.flatMap(
            new FlatMapFunction<String, String>() {
              @Override
              public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(" ");
                return Arrays.asList(words).iterator();
              }
            });
    System.out.println("flatMap " + flatMap.collect());

    JavaPairRDD<String, Integer> pair =
        flatMap.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
              }
            });
    pair.foreach(
        new VoidFunction<Tuple2<String, Integer>>() {
          @Override
          public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            System.out.println(stringIntegerTuple2);
          }
        });

    JavaPairRDD<String, Integer> reduceRDD =
        pair.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
              }
            });
    reduceRDD.foreach(
        new VoidFunction<Tuple2<String, Integer>>() {
          @Override
          public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            System.out.println(stringIntegerTuple2);
          }
        });
    // 按key排序
    JavaPairRDD<String, Integer> sortByKey = reduceRDD.sortByKey(true);
    sortByKey.foreach(
        new VoidFunction<Tuple2<String, Integer>>() {
          @Override
          public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            System.out.println(stringIntegerTuple2);
          }
        });

    // 按value排序
    JavaPairRDD<Integer, String> pair1 =
        sortByKey.mapToPair(
            new PairFunction<Tuple2<String, Integer>, Integer, String>() {
              @Override
              public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
              }
            });
    JavaPairRDD<Integer, String> sortByValue = pair1.sortByKey(false);
    JavaPairRDD<String, Integer> sortByValueRDD =
        sortByValue.mapToPair(
            new PairFunction<Tuple2<Integer, String>, String, Integer>() {

              @Override
              public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
              }
            });
    sortByValueRDD.foreach(
        new VoidFunction<Tuple2<String, Integer>>() {
          @Override
          public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            System.out.println(stringIntegerTuple2);
          }
        });

    /** 处理结果 */
    sortByValueRDD.saveAsTextFile("C:\\Users\\wyg04\\Desktop\\wordReslut");

    /** 关闭资源 */
    sc.stop();
  }
}
