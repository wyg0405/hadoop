package com.wyg.spark.day3.lagou

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Description: 
 * 5、求出不同标签的公司数量和招聘数量（只输出招聘需求最大的50个标签）
 *
 * 结果样式：
 * 高级,5232,1414
 * 金融,4865,995
 * 资深,3717,1080
 * Java,3531,1154
 * 大数据,3375,831
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-29 12:24
 * @version V1.0
 */

object Answer5 extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("lagou2").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val textRdd: RDD[String] = sc.textFile("C:\\Users\\wyg04\\Desktop\\lagou\\lagou.txt")
    val tagRDD: RDD[(String, String)] = textRdd.map(line => {
      line.split("\\^")
    }).flatMap(x => {
      val arrayBuffer: ArrayBuffer[(String,String)] = new ArrayBuffer[(String, String)]()
      x(3).split(",").foreach(y => {
        arrayBuffer.append((y, x(4)))
      })
      arrayBuffer.iterator
    })
    val resultRDD: RDD[(String, Int, Int)] = tagRDD.groupByKey().map(x => {
      val tag: String = x._1
      val value: Iterable[String] = x._2
      val length: Int = value.toList.length

      val company: Int = value.toList.distinct.length
      (tag, length, company)
    })
    val result: Array[(String, Int, Int)] = resultRDD.sortBy(x=>x._2,false).take(50)

    for (elem <- result) {
      println(elem)
    }
    sc.stop()
  }
}
