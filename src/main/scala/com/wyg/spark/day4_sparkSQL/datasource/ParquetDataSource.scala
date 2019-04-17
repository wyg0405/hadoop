package com.wyg.spark.day4_sparkSQL.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-15 11:20
 * @version V1.0
 */

object ParquetDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("read from mysql")
      .master("local[*]")
      .getOrCreate()
    //方式一
    val stu: DataFrame = spark.read.parquet("C:\\Users\\wyg04\\Desktop\\stu_parquet")
    //parquet文件既保存了数据也保存了schema，还保存了偏移量，列式存储
    stu.printSchema()
    stu.show()
    //方式二
    val stu2 = spark.read.format("parquet").load("C:\\Users\\wyg04\\Desktop\\stu_parquet")
    stu2.printSchema()
    stu2.show()
    spark.stop()
  }

}
