package com.wyg.spark.day3.lagou.answer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 作者： 马中华   https://blog.csdn.net/zhongqi2513
  * 时间： 2018/8/3 13:10
  *


数据样式：
1	  Java高级	  北苑  信息安全,大数据,架构 闲徕互娱  20k-40k	  本科	经验5-10年 游戏  不需要融资

字段：
id	job		    addr		tag			          company		salary		edu	   exp		type		level

中文解释
id	工作岗位	   地址		标签			          公司		     薪资		  学历	   经验		 类型		融资级别




  */
object LagouModel {

  def main(args: Array[String]): Unit = {


    /**
      * 第一步： 获取程序入口
      */
    val sparkConf = new SparkConf().setAppName("LagouModel").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")


    /**
      * 第二步： 获取RDD
      */
    val lineSRDD: RDD[String] = sparkContext.textFile("hdfs://myha01/lagou/input/")


    /**
      * 第三步： 做逻辑处理
      */


    /**
      * 第四步： 回收资源
      */
    sparkContext.stop()

  }
}
