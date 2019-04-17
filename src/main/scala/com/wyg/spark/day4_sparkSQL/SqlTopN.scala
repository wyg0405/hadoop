package com.wyg.spark.day4_sparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Description: 用sql求TopN
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-15 13:38
 * @version V1.0
 */

object SqlTopN {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sql TopN")
      .master("local[*]")
      .getOrCreate()
    val log: Dataset[String] = spark.read.textFile("C:\\Users\\wyg04\\Desktop\\sparkdata\\teacher.log")
    import spark.implicits._
    val logDf: DataFrame = log.map(line => {
      val teacher = line.substring(line.lastIndexOf("/") + 1)
      val course = line.substring(line.lastIndexOf("//") + 2, line.indexOf("."))
      (course, teacher)
    }).toDF("course", "teacher")

    logDf.createTempView("v_log")
    //先分组
    val count = spark.sql("select course ,teacher,count(*) count from v_log group by course,teacher ").createTempView("v_count")
    //排序
    val result1: DataFrame = spark.sql("select course,teacher,count, row_number() over(partition by course order by count desc) rn from v_count ")
    result1.show()

    //top1
    val result2 = spark.sql("select * from( select course,teacher,count, row_number() over(partition by course order by count desc) rn from v_count order by course) where rn<=1")
    result2.show()

    //全局排序
    val result3 = spark.sql(" select course,teacher,count, row_number() over(partition by course order by count desc) groupRank , row_number() over(order by count desc) globaRank from v_count order by globaRank ")
    result3.show()
  }
}
