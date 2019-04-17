package com.wyg.spark.day4_sparkSQL.SparkSQL2.x

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-11 20:37
 * @version V1.0
 */

object SqlWordCount {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("SqlWordCount")
      .master("local[*]")
      .getOrCreate()

    //dataset只有一列value
    val ds: Dataset[String] = session.read.textFile("hdfs://hadoop1:9000/input/stu/student.txt")
    //ds.show()
    //整理数据
    import session.implicits._
    val flatMap: Dataset[String] = ds.flatMap(lines => lines.split("\t"))
    //创建视图
    flatMap.createTempView("v_wc")
    //执行sql
    val df: DataFrame = session.sql("select value,count(*) count from v_wc group by value order by count desc")
    df.show()
    session.stop()
  }
}
