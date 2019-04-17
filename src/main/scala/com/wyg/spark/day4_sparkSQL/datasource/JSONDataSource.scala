package com.wyg.spark.day4_sparkSQL.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Description: 读取JSON
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-15 11:11
 * @version V1.0
 */

object JSONDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("read from mysql")
      .master("local[*]")
      .getOrCreate()
    val stu: DataFrame = spark.read.json("C:\\Users\\wyg04\\Desktop\\stu_json")
    stu.printSchema()
    stu.show()
    spark.stop()
  }
}
