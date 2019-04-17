package com.wyg.spark.day4_sparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Description: sparkSql join
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-12 10:36
 * @version V1.0
 */

object SqlJoin1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("join")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val stu: Dataset[String] = spark.createDataset(List("1,tom,usa", "2,lilei,cn", "3,jack,uk", "4,noone,rus"))
    val stuDS: Dataset[(Long, String, String)] = stu.map(lines => {
      val line = lines.split(",")
      val id = line(0).toLong
      val name = line(1).toString
      val country = line(2).toString
      (id, name, country)
    })
    //
    val stuDF: DataFrame = stuDS.toDF("id", "name", "country")
    stuDF.show()
    val country = spark.createDataset(List("1,cn,中国", "2,usa,美国", "3,uk,英国", "4,jp,日本"))
    val countryDS: Dataset[(Long, String, String)] = country.map(lines => {
      val line = lines.split(",")
      val id = line(0).toLong
      val code = line(1).toString
      val name = line(2).toString
      (id, code, name)
    })
    val countryDF: DataFrame = countryDS.toDF("id", "code", "name")
    countryDF.show()
    //用sql join
    stuDF.createTempView("v_stu")
    countryDF.createTempView("v_country")
    val sqlResult: DataFrame = spark.sql("select v1.id,v1.name,v2.name as country from v_stu v1 left join v_country v2 on v1.country=v2.code")
    sqlResult.show()

    //用DataFrame API
    val dsResult: DataFrame = stuDF.join(countryDF, $"country" === $"code", "left")
    dsResult.show()
    spark.stop()
  }
}
