package com.wyg.spark.day3.lagou

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Description: 
 * 求出不同融资级别的公司，在招聘大数据类工作岗位时，在学历和经验一致的条件下，薪资会高出多少。
 *
 * 本科		1-3年	A轮-B轮		2000-3000
 * 本科		1-3年	B轮-C轮		2100-4500
 * .......
 * 本科		3-5年	A轮-B轮		3000-6000
 * 本科		3-5年	B轮-C轮		4000-5000
 * .....
 * 研究生		1-3年	A轮-B轮		4000-5000
 * ......
 *
 * (4000-5000)  ------  (平均最少薪资-平均最大薪资)
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-28 22:30
 * @version V1.0
 */

object bject

object Answer4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("lagou2").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val textRdd: RDD[String] = sc.textFile("C:\\Users\\wyg04\\Desktop\\lagou\\lagou.txt")
    //按education分组
    val edu: RDD[(String, Iterable[(String, String, String)])] = textRdd.map(x => x.split("\\^")).map(x => (x(6), (x(5), x(7), x(9)))).groupByKey()
    var salary: Array[String] = null
    var minSalary: Double = 0.0
    var maxSalary: Double = 0.0
    var invalid: Int = 0

    edu.collect.map(x => {
      //按exp分组
      val exp: RDD[(String, Iterable[(String, String)])] = sc.makeRDD(x._2.toArray).map(y => ((y._2, (y._1, y._3)))).groupByKey()
      exp.collect.map(z => {
        //按level分组
        val level: RDD[(String, (Double, Double))] = sc.makeRDD(z._2.toArray).map(a => {
          salary = a._1.split("-")
          if (salary.length == 2 && salary(0).indexOf("k") > 0 && salary(1).indexOf("k") > 0 && salary(0).indexOf("k") != salary(1).indexOf("k")) {
            minSalary = salary(0).substring(0, salary(0).indexOf("k")).toDouble
            maxSalary = salary(1).substring(0, salary(1).indexOf("k")).toDouble
          } else {
            //无效数据
            invalid += 1
          }

          (a._2, (minSalary, maxSalary))
        }).aggregateByKey((0.0, 0.0, 0))((b, c) => (b._1 + c._1, b._2 + c._2, b._3 + 1), (b, c) => (b._1 + c._1, b._2 + c._2, b._3 + c._3)).map(d => (d._1, (d._2._1 / (d._2._3 - invalid), d._2._2 / (d._2._3 - invalid))))
        level.foreach(e => println(x._1, z._1, e))

      })
    })
    sc.stop()
  }
}
