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


需求：
 4、求出不同融资级别的公司，在招聘大数据类工作岗位时，在学历和经验一致的条件下，薪资会高出多少。
本科		1-3年	A轮		10K-20K
本科		1-3年	B轮		12K-25K
.......
本科		3-5年	A轮		15K-25K
本科		3-5年	B轮		16K-30K
.....
研究生	1-3年	A轮		20K-30K
......

(10K-20K)  ------  (平均最少薪资-平均最大薪资)

分析需求：

本科	3-5年	A	B	2k
本科	3-5年	B	C	1.5k
本科	3-5年	C	D	2k
本科	3-5年	D	天使轮	2k

本科	3-5年	A	C	2k
本科	3-5年	A	D	2k
本科	3-5年	A	天使轮	2k

本科	3-5年	21k	23K	25K	30K	50K

 D:\bigdata\lagou\input\lagou.txt
 */
object Lagou4 {

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
		val linesRDD: RDD[String] = sparkContext.textFile("C:\\Users\\wyg04\\Desktop\\lagou\\lagou.txt")


		/**
      * 第三步： 对RDD 进行操作
      */
    val fieldsRDD: RDD[Array[String]] = linesRDD.map(x => x.split("\\^"))
		val bigdataRDD: RDD[Array[String]] = fieldsRDD.filter(x =>
			x(1).contains("大数据") | x(1).toLowerCase.contains("hadoop") | x(1).toLowerCase.contains("spark"))

		val keyValueRDD: RDD[((String, String), (String, Int, Int))] = bigdataRDD.map(x => ((x(6), x(7), x(9)), x(5))).groupByKey().map(x => {

			// x ====>  [(String, String, String), Iteable[String]]
			val key = x._1
			val value: Iterable[String] = x._2

			// 累计 该 学历和经验级别下 工作岗位的数量
			var count = 0
			var max_sum = 0    // 累计所有的上限工资
			var min_sum = 0    // 累计所有的下限工资
			val iterator = value.iterator
			while (iterator.hasNext) {
				val str: String = iterator.next()
				val min_max = str.split("-")

				// 计算上限工资总和     和     下限工资总和
				min_sum += (min_max(0).substring(0, min_max(0).length - 1).toInt * 1000)
				max_sum += (min_max(1).substring(0, min_max(1).length - 1).toInt * 1000)
				// 计数
				count += 1
			}

			// 计算下限工资的平均 和 上限工资的平均
			val avgmin = Math.round(min_sum / count)
			val avgmax = Math.round(max_sum / count)

			// 学历   经验       融资    下限    上限
			((key._1, key._2), (key._3, avgmin, avgmax))
		})


		val lastResultRDD: RDD[(String, String, String, String)] = keyValueRDD.groupByKey().flatMap(x => {
			val buffer: ArrayBuffer[(String, String, String, String)] = new ArrayBuffer[(String, String, String, String)]()
			//  x ==== [((String, String), Iterable[(String, Int, Int)])]
			val key = x._1
			val value: Iterable[(String, Int, Int)] = x._2
			val list: List[(String, Int, Int)] = value.toList
			for (i <- 0 to list.length - 2) {
				for (j <- i + 1 to list.length - 1) {

					// 学历
					val xueli = key._1
					// 经验
					val jignyan = key._2
					val wai: (String, Int, Int) = list(i)
					val nei: (String, Int, Int) = list(j)
					// 融资级别
					val rongzi = wai._1 + "-" + nei._1
					var xiaxian = nei._2 - wai._2
					var shangxian = nei._3 - wai._3
					// 薪资
					var xinzi = xiaxian + "-" + shangxian

					// 本科    1-3      A-B          2-4
					buffer.append((xueli, jignyan, rongzi, xinzi))
				}
			}

			// 因为是flatMap所以要返回一个 迭代器
			buffer.toIterator
		}).sortBy(x => (x._1, x._2))

		lastResultRDD.foreach(x => println(x))
    /**
      * 第四步： 关闭sparkContext
      */
		sparkContext.stop
  }
}
