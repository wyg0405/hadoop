package com.wyg.spark.day2

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * Description: 累加器
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-26 14:55
 * @version V1.0
 */

object Accumulators {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("Accumulators").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    var sum: LongAccumulator = sc.longAccumulator("sum")
    val rdd: RDD[Int] = sc.makeRDD(1 to 20)
    val map: Array[LongAccumulator] = rdd.map(x => {
      //print(x)
      sum.add(x)
      sum
    }).collect()
    map.foreach(x=>println(x.value))
    print(sum.value)

  }


}
