package com.wyg.spark.day4_sparkSQL.SparkSQL1.x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-11 17:48
 * @version V1.0
 */

object SparkSQL3 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL 1.x").setMaster("local[*]")

    //创建入口
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val stu = sc.textFile("hdfs://hadoop1:9000/input/stu/student.txt")
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

    //将RDD关联schema
    val stuDF: DataFrame = sqlContext.createDataFrame(stuRDD, schema)
    //不使用sql方式，就不用注册临时表
    val df: DataFrame = stuDF.select("id", "name", "age")
    df.show()
    import sqlContext.implicits._
    val df2: DataFrame = stuDF.orderBy($"age" asc, $"name" desc)
    df2.show()
    sc.stop()


  }
}
