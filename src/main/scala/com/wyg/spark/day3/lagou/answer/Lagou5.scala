package com.wyg.spark.day3.lagou.answer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**

数据样式：
1	Java高级	北苑		信息安全,大数据,架构	闲徕互娱	20k-40k		本科	经验5-10年	游戏		不需要融资

字段：
id	job		addr		tag			company		salary		edu	exp		type		level

中文解释
id	工作岗位	地址		标签			公司		薪资		学历	经验		类型		融资级别


5、求出不同标签的公司数量和招聘数量（只输出招聘需求最大的50个标签）

结果样式：
带薪年假		300		500
文化娱乐		200		222
........

标签	招聘数量	公司排序

具体实现思路：

  1、先拆分标签。
  2、按照标签分组。
  3、每组中的记录条数，就是这个标签的招聘数量
  4、每组中的所有公司去重了之后，在计数，就是这个标签的的公司个数
  5、然后按照招聘数量降序排序。
  6、取50条记录

  */
object Lagou5 {

  def main(args: Array[String]): Unit = {
    /**
      * 第一步： 获取编程入口
      * SparkCore  SparkContext
      */
    val sparkConf = new SparkConf().setAppName("Lagou4").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    /**
      * 第二步：获取数据抽象
      */
//    val linesRDD: RDD[String] = sparkContext.textFile("file:///D:\\bigdata\\lagou\\input\\lagou.txt")
    val linesRDD: RDD[String] = sparkContext.textFile("hdfs://myha01/lagou/input/")

    /**
      * 第三步： 处理linesRDD
      */
    val fieldsRDD: RDD[Array[String]] = linesRDD.map(x => x.split("\\^"))

    // x 是数组                      标签   1    公司名称
    val tagOneComRDD: RDD[(String, (Int, String))] = fieldsRDD.flatMap(x => {
      //               标签  1次招聘   公司
      val buffer: ArrayBuffer[(String, (Int, String))] = new ArrayBuffer[(String, (Int, String))]()

      // 拆分标签
      val tags = x(3).split(",")
      for (tag <- tags) {
        // 组装记录：  标签， 1， 公司名称
        buffer.append((tag, (1, x(4))))
      }
      buffer.toIterator
    })

    // 按照标签分组， 求出每个标签的招聘数量， 和  公司数量
    val lastResultRDD:RDD[(String, Int, Int)] = tagOneComRDD.groupByKey().map(x => {
      // x === Iterable[(Int, String)]

      // 标签
      val tag = x._1
      //                             招聘次数1次， 公司名称
      // 招聘的必要数据集合　　Iterable[(Int,      String)]
      val values = x._2

      // 招聘数量， 就是这个 Iterable的长度
      var count = values.toList.length

      // 公司数量， 就是(Int,      String)]中String数据去重之后的个数。
      val coms: Iterable[String] = values.map(x => x._2)
      val com_numer = coms.toList.distinct.length

      // 最后返回的元素值：  标签，招聘数量，公司数量
      (tag, count, com_numer)
    })

//    lastResultRDD.foreach(x => println(x))

    // 按照招聘数量降序排序。 取前50   输出结果。！
    val lastResultArray: Array[(String, Int, Int)] = lastResultRDD.sortBy(_._2, false).take(50)
    lastResultArray.foreach(x => println(x))

    // 关闭sparkCOntext
    sparkContext.stop()
  }
}
