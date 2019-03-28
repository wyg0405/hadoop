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
object MovieRate6 {

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
      * 6、求最喜欢看电影（影评次数最多）的那位女性评最高分的10部电影
      * 的平均影评分（观影者，电影名，影评分）
      *
      * 实现步骤：
      *
      *   1、求最喜欢看电影的人。 女性  观影次数最多
      *
      *   2、该女性觉得好看的10部电影。！！！
      *
      *   3、求这10部电影的评分
      */

    val userid_rdd = rdd_rate.map(x => {
      val xx = x.split("::")
      val userid = xx(0)
      (userid, 1)
    })

    val user_id_sex_rdd = rdd_user.map(x => {
      val xx = x.split("::")
      val userid = xx(0)
      (userid, xx(1))
    }).filter(_._2 == "F")

    // userid,  sex,   1
    val rate_userid_sex_rdd: RDD[(String, (String, Int))] = user_id_sex_rdd.join(userid_rdd)

    val user_rates_rdd: RDD[(String, Int)] = rate_userid_sex_rdd.map(x => {
      (x._1, 1)
    }).reduceByKey(_ + _).sortBy(_._2, false)

    val oneResult: Array[(String, Int)] = user_rates_rdd.take(1)

    val userid_f = oneResult(0)._1
    println(userid_f)


    /**
      * 求这个女性的最喜欢看的10部电影
      */

      // 就是这个女性最喜欢看的10个电影！！！！

      // userid， movieid，  rate
      //  1150     xxx       5.0
    val top10_f: Array[(String, String, Int)] = rdd_rate.map(x => {
      val xx = x.split("::")

        // userid,   rate
      (xx(0), xx(1),  xx(2).toInt)
    }).filter(x => {
      x._1 == userid_f
    }).sortBy(_._3, false).take(10)

    val movieids: Array[String] = top10_f.map(x => x._2)

    val top10_movie_rate: RDD[(String, Double)] = rdd_rate.map(x => {
      val xx = x.split("::")
      val movieid = xx(1)
      val rate = xx(2)
      (movieid, rate.toInt)
    }).filter(x => {
      movieids.contains(x._1)
    }).groupByKey().map(x => {

      // x ==== (String, Iterable[Int])
      val movieid = x._1
      val sum = x._2.sum
      val size = x._2.toList.size
      val avgrate = sum * 1D / size
      (movieid, avgrate)
    })
    top10_movie_rate.foreach(x => {
      println(x._1, x._2)
    })

    sparkContext.stop()
  }
}
