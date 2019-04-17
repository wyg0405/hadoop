package com.wyg.spark.day4_sparkSQL.SparkSQL2.x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-11 18:09
 * @version V1.0
 */

object SparkSQL1 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("SparkSQL2.x")
      .master("local[*]")
      .getOrCreate()

    //创建RDD
    val stu: RDD[String] = session.sparkContext.textFile("hdfs://hadoop1:9000/input/stu/student.txt")
    val stuRDD: RDD[Row] = stu.map(line => {
      val info = line.split("\t")
      val id = info(0).toLong
      val name = info(1).toString
      val age = info(2).toInt
      val gender = info(3).toString
      val major = info(4).toString

      Row(id, name, age, gender, major)
    })
    //结果类型，用于描述DataFrame
    val schema = StructType(List(
      StructField("id", LongType, false),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("gender", StringType, true),
      StructField("major", StringType, true)
    ))
    val df: DataFrame = session.createDataFrame(stuRDD, schema)
    df.show()
    import session.implicits._
    val df2: Dataset[Row] = df.where($"age" === 12).orderBy($"major")
    df2.show()
    //df2.write.json()
  }
}
