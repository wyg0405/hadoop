package com.wyg.spark.day3.lagou

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-28 21:03
 * @version V1.0
 */

object Test {
  def main(args: Array[String]): Unit = {
    val a="1^Java高级^北苑^信息安全,大数据,架构^闲徕互娱^20k-40k^本科^经验5-10年^游戏^不需要融资"
    val b="3^Java开发工程师^北京大学^专家,总监,资深,高级,C/C++,大数据^zilliz^15k-30k^本科^经验3-5年^企业服务,数据服务^A轮"
    println(a.indexOf("大数据"))
    println(b.substring(0,b.indexOf("k")))
  }
}
