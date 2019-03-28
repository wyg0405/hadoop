package com.wyg.spark.day3.movie

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-27 15:53
 * @version V1.0
 */

object Test {
  def main(args: Array[String]): Unit = {
    val a="3620::Myth of Fingerprints, The (1997)::Comedy|Drama"

    println(a.substring(a.lastIndexOf("(")+1,a.lastIndexOf(")")))
    var b="1404::Night Falls on Manhattan (1997)::Crime|Drama"

     val c: Array[String] = b.split("::")
    println(c.length)

    val str: String = c(2)
    println(str)
    println(new String(c(2)))
    println(str.split("m").length)

    val d: Boolean = c(2).split("|").contains("Crime")
    val e: Boolean = c(2).split("|").exists(x=>x=="Crime")
    val f: Boolean = c(2).split("|").toList.contains("Crime")
    val g: Boolean = str.split("|").toList.contains("Crime")
    println(d)
    println(e)
    println(f)
    println(g)
    val iterator: Iterator[Char] = c(2).iterator


    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))
    //rdd.flatMap()
  }

}
