package com.wyg.spark.day2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-26 14:20
 * @version V1.0
 */

object BroadCastVar {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("BroadCast Varibales").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val text: RDD[String] = sc.textFile("C:\\Users\\wyg04\\Desktop\\word.txt")
    val a: Int = 1
    val b: Broadcast[Int] = sc.broadcast(a)
    val map: RDD[(String, Int)] = text.flatMap(x => x.split(" ")).map(x => (x, 1))
    map.map(x => (x._1, x._2 + a)).foreach(print(_))
    println
    map.map(x => (x._1, x._2 + b.value)).foreach(print(_))
  }
}
