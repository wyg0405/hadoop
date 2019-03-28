package com.wyg.spark.day3.movie.answer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**

1、users.dat    数据格式为：  2::M::56::16::70072
对应字段为：UserID BigInt, Gender String, Age Int, Occupation String, Zipcode String
对应字段中文解释：用户id，性别，年龄，职业，邮政编码

2、movies.dat		数据格式为： 2::Jumanji (1995)::Adventure|Children's|Fantasy
对应字段为：MovieID BigInt, Title String, Genres String
对应字段中文解释：电影ID，电影名字，电影类型

3、ratings.dat		数据格式为：  1::1193::5::978300760
对应字段为：UserID BigInt, MovieID BigInt, Rating Double, Timestamped String
对应字段中文解释：用户ID，电影ID，评分，评分时间戳

  */
object MovieRate4 {

  def main(args: Array[String]): Unit = {

    /**
      * 初始化SparkContext
      */
    val sparkConf = new SparkConf().setAppName("MoDEL").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val rdd_user: RDD[String] = sparkContext.textFile("D:\\bigdata\\user\\users.dat")
    val rdd_movie: RDD[String] = sparkContext.textFile("D:\\bigdata\\movie\\movies.dat")
    val rdd_rate: RDD[String] = sparkContext.textFile("D:\\bigdata\\rate\\ratings.dat")


    /**
      * 实现需求：
      *
      * 4、年龄段在“18-24”的男人，最喜欢看的10部电影
      *
      *   实现步骤：
      *
      *   1、在构建  user——rdd的时候xx，就需要把  年龄和性别作为过滤条件，
      *
      *   2、按照评论次数降序排序， 取前10
      */

    val m_user_rdd: RDD[(String, String)] = rdd_user.map(x => {
      val xx = x.split("::")

      //  哦用户ID，  性别，  年龄
      (xx(0), xx(1), xx(2).toInt)
    }).filter(x => {
      x._2 == "M" && (x._3 >= 18 && x._3 <= 24)
    }).map(x => {
      (x._1, x._2)
    })


    val rate_user_movie: RDD[(String, (String, Int))] = rdd_rate.map(x => {
      val xx = x.split("::")

      // 用户ID，  电影ID，  评分
      (xx(0), (xx(1), xx(2).toInt))
    })

    val movie_id_rate: RDD[(String, Int)] = rate_user_movie.join(m_user_rdd).map(x => {
      //      x   =====   (String, ((String, Int), String))
      (x._2._1._1, x._2._1._2)
    })

    val sorted_top10_movie_sex: RDD[(String, Double)] = movie_id_rate.groupByKey().map(x => {
      // X ==== (String, Iterable[Int])
      val movieid = x._1
      val sum = x._2.sum
      val size = x._2.toList.size
      val avgrate = sum * 1D / size
      (movieid, avgrate)
    }).sortBy(_._2, false)


    val result = sorted_top10_movie_sex.take(10)

    // 只有电影ID， 和 评分
    result.foreach(x => {
      println("M", x._1, x._2)
    })

    sparkContext.stop()
  }
}
