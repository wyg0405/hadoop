package com.wyg.spark.day3.lagou

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Description: 使用学历和经验作为联合分组
 *比如，各种学历不同经验的平均薪水
 * @author: wyg0405@gmail.com
 * @date: 2019-03-28 20:24
 * @version V1.0
 */

object Answer2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("lagou2").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("C:\\Users\\wyg04\\Desktop\\lagou\\lagou.txt")
    //data.map(line=>line.split("\\^")).
  }
}
