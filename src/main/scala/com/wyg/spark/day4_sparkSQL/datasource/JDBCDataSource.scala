package com.wyg.spark.day4_sparkSQL.datasource

import java.util.Properties

import org.apache.spark.sql._

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-12 21:07
 * @version V1.0
 */

object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("read from mysql")
      .master("local[*]")
      .getOrCreate()
    val stu: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://hadoop1:3306/spark",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "stu",
        "user" -> "hadoop",
        "password" -> "123456")).load()
    stu.printSchema()
    stu.show()
    //api
    val result1 = stu.filter(row => {
      row.getAs[Int](2) > 13
    })
    result1.show()
    //lambda
    import spark.implicits._
    val resutl2: Dataset[Row] = stu.filter($"age" > 13)
    resutl2.show()
    //sql
    stu.createTempView("v_stu")
    val result3: DataFrame = spark.sql("select * from v_stu where age>13")
    result3.show()
    /** *********写入mysql ******************/
    val result4 = stu.select($"id" * 10 as "id", $"name", $"age" * 10 as "age")
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.setProperty("password", "123456")
    //写入mysql，不用新建表，DataFrame保留了表信息
    //result4.write.mode("ignore").jdbc("jdbc:mysql://hadoop1:3306/spark","sut1",prop)

    //text保存
    /**
     * text只能保存单列的，多列会丢失schame信息
     * Caused by: org.apache.spark.sql.AnalysisException: Text data source supports only a single column, and you have 3 columns.;
     */
    //result4.write.text("C:\\Users\\wyg04\\Desktop\\stu.txt")


    /**
     * 只能保存string类型的数据
     * Caused by: org.apache.spark.sql.AnalysisException: Text data source supports only a string column, but you have int.;
     */
    val result5 = stu.select($"id")
    //result5.write.text("C:\\Users\\wyg04\\Desktop\\stu_txt")
    val result6 = stu.select($"name")
    //result6.write.text("C:\\Users\\wyg04\\Desktop\\stu_txt")

    //保存为json
    //result4.write.json("C:\\Users\\wyg04\\Desktop\\stu_json")

    //保存为csv
    //result4.write.csv("C:\\Users\\wyg04\\Desktop\\stu_csv")

    //保存为parquet
    result4.write.parquet("C:\\Users\\wyg04\\Desktop\\stu_parquet")


    spark.stop()

  }
}
