package com.wyg.spark.day4_sparkSQL.ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Description:
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-12 12:05
 * @version V1.0
 */

object IpLocationSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IpLocaltionSql")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ipRule: Dataset[String] = spark.read.textFile("hdfs://hadoop1:9000/input/ip/ip.txt")

    //整理ip，转成DataFrame
    val ipDS: Dataset[(Long, Long, String)] = ipRule.map(lines => {
      val line = lines.split("\\|")
      val start = line(2).toLong
      val end = line(3).toLong
      val province = line(6).toString
      (start, end, province)
    })
    //ipDF.show(10)
    val ipDF = ipDS.toDF("startNum", "endNum", "province")

    //创建访问日志Dataset
    val log: Dataset[String] = spark.read.textFile("hdfs://hadoop1:9000/input/ip/access.log")
    val logDF: DataFrame = log.map(lines => {
      val line = lines.split("[|]")
      val ip = line(1)
      //将ip转为十进制
      ip2Long(ip)
    }).toDF("ip")
    logDF.show(10)

    //用sql方式
    /*ipDF.createTempView("v_ip")
    logDF.createTempView("v_log")
    //join代价太昂贵，而且非常慢，解决思路是将表缓存起来（广播变量）
    val sqlResut: DataFrame = spark.sql("select v2.province,count(*) count from v_log v1 join v_ip v2 on (v1.ip>=v2.startNum and v1.ip<=v2.endNum) group by v2.province order by count desc")
    sqlResut.show()*/

    //改造
    //将分散在多个Executors中的部分Ip规则收集到Driver中，并广播
    val ipRows: Array[(Long, Long, String)] = ipDS.collect()
    val ipBC: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(ipRows)
    logDF.createTempView("v_log")
    spark.udf.register("ip2Province", (ip: Long) => {
      val ipRule: Array[(Long, Long, String)] = ipBC.value
      //根据IP地址对应的十进制查找省份名称
      val index = binarySearch(ipRule, ip)
      var province = "未知"
      if (index != -1) {
        province = ipRule(index)._3
      }
      province
    })
    val result = spark.sql("select ip2Province(ip) province,count(*) count from v_log group by province order by count desc ")
    result.show()
    spark.stop()
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
}
