package com.wyg.spark.day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * wordcount
 */
object WordCount {

  /**
   * @Description:
   * @param:
   * @return:
   * @throws:
   * @author: wyg0405@gmail.com
   * @date: 2019/3/22 14:11
   */

  def main(args: Array[String]): Unit = {

    val inputFile: String = "C:\\Users\\wyg04\\Desktop\\word.txt"
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val textFile: RDD[String] = sc.textFile(inputFile)
    //val textFile: RDD[String] = sc.textFile(inputFile)
    val wordcount: RDD[(String, Int)] = textFile.flatMap((line: String) => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordcount.collect().foreach((word: (String, Int)) =>println(word))
    val unit: Unit = wordcount.saveAsTextFile("C:\\Users\\wyg04\\Desktop\\reslut")
    //wordcount.saveAsTextFile(args(2))
    sc.stop()
  }

}
