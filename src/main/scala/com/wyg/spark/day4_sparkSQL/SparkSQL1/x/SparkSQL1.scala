package com.wyg.spark.day4_sparkSQL.SparkSQL1.x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Description: sparkSQL 1.x的使用
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-11 16:05
 * @version V1.0
 */

object SparkSQL1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL 1.x").setMaster("local[*]")

    //创建入口
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val stu = sc.textFile("hdfs://hadoop1:9000/input/stu/student.txt")
    val stuRDD: RDD[Student] = stu.map(line => {
      val info = line.split("\t")
      val id = info(0).toLong
      val name = info(1).toString
      val age = info(2).toInt
      val gender = info(3).toString
      val major = info(4).toString
      Student(id, name, age, gender, major)
    })
    //导入隐式转换
    import sqlContext.implicits._
    //将RDD转换为DataFrame
    val stuDF: DataFrame = stuRDD.toDF()
    //DataFrame注册临时表
    stuDF.registerTempTable("t_stu")
    //写SQL,相当于Transformation
    val result: DataFrame = sqlContext.sql("select * from t_stu t order by t.age")
    //相当于Action
    result.show()

    sc.stop()

  }
}

case class Student(id: Long, name: String, age: Int, gender: String, major: String)
