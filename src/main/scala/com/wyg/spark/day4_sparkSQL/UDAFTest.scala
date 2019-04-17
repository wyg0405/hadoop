package com.wyg.spark.day4_sparkSQL

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Description: 自定义聚合函数,几何平均数
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-12 15:58
 * @version V1.0
 */

object UDAFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UDAF")
      .master("local[*]")
      .getOrCreate()
    val geo = new GeoMean
    //注册自定义函数
    spark.udf.register("geo", geo)
    val range = spark.range(1, 3)
    //默认列值为id
    //sql
    range.createTempView("range")
    val result: DataFrame = spark.sql("select geo(id) result from range")
    result.show()
    //API
    import spark.implicits._
    val result2: DataFrame = range.agg(geo($"id") as "result")
    result2.show()
    spark.stop()
  }
}

class GeoMean extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = StructType(List(
    StructField("value", DoubleType)
  ))

  //中间数据类型
  override def bufferSchema: StructType = StructType(List(
    //每个分区的数据数
    StructField("count", LongType),
    //乘积
    StructField("product", DoubleType)
  ))

  //最终结果类型
  override def dataType: DataType = DoubleType

  //确保一致性
  override def deterministic: Boolean = true


  //后面这些就像RDD中aggregateByKey算子一样
  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  //每条数据和中间结果的操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1
    buffer(1) = buffer.getDouble(1) * input.getDouble(0)

  }

  //每个分区合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getDouble(1) * buffer2.getDouble(1)
  }


  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}
