package com.wyg.spark.day4_sparkSQL.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-15 11:16
 * @version V1.0
 */

object CSVDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("read from mysql")
      .master("local[*]")
      .getOrCreate()
    val stu: DataFrame = spark.read.csv("C:\\Users\\wyg04\\Desktop\\stu_csv")
    //无schema信息
    stu.printSchema()
    stu.show()
    //手动加上schema
    stu.toDF("id", "name", "age").show()
    spark.stop()
  }
}
