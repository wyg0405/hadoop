package com.wyg.spark.day4_sparkSQL.SparkSQL2.x

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-11 22:38
 * @version V1.0
 */

object DatasetWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSetWordCount")
      .master("local[*]")
      .getOrCreate()
    val ds: Dataset[String] = spark.read.textFile("hdfs://hadoop1:9000/input/stu/student.txt")
    import spark.implicits._
    val flatMap: Dataset[String] = ds.flatMap(lines => lines.split("\t"))
    //flatMap.show()
    //使用Dataset的API
    val result: Dataset[Row] = flatMap.groupBy($"value" as "word").count().orderBy($"count" desc)
    result.show()
    //使用聚合函数
    //导入聚合函数
    import org.apache.spark.sql.functions._
    var result2 = flatMap.groupBy($"value" as "word").agg(count("*") as "mycount").orderBy($"mycount" desc)
    result2.show()
    spark.stop()
  }
}
