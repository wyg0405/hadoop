package com.wyg.spark.day3.lagou

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Description: 求出每个不同地点的招聘数量，输出需求量最大的10个地方，以及岗位需求量
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-28 18:15
 * @version V1.0
 */

object Answer1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("answer1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("C:\\Users\\wyg04\\Desktop\\lagou\\lagou.txt")
    val groupByAdrr: RDD[(String, Int)] = data.map(line => line.split("\\^")).map(line => (line(2), 1)).reduceByKey((x, y) => x + y).sortBy(x=>x._2,false)
    println("每个不同地点的招聘数量")
    groupByAdrr.foreach(println(_))
    println("需求量最大的10个地方")
    groupByAdrr.take(10).foreach(println)
    sc.stop()
  }
}
