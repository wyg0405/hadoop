package com.wyg.spark.day3.lagou

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Description: 
 * 3、求出大数据类工作（job字段包含"大数据"，"hadoop"，"spark"就算大数据类工作）岗位对学历的要求，不同学历的需求量和占比
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-28 20:46
 * @version V1.0
 */

object Answer3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("lagou2").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val textRdd: RDD[String] = sc.textFile("C:\\Users\\wyg04\\Desktop\\lagou\\lagou.txt")
    //筛选大数据岗位
    val bigdata: RDD[(String, String)] = textRdd.map(x => x.split("\\^")).map(x => {
      (x(1), x(6))
    }).filter(x => (x._1.indexOf("大数据") > 0 || x._1.indexOf("hadoop") > 0 || x._1.indexOf("spark") > 0))
    //大数据总岗位
    val totalRecord: Long = bigdata.count()
    //按学历分组
    val education: RDD[(String, Int)] = bigdata.map(x=>(x._2,1)).reduceByKey((x,y)=>x+y)
    education.map(x=>{
      (x._1,(x._2.toDouble/totalRecord))
    }).foreach(println)
    sc.stop()
  }
}
