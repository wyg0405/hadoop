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
object MovieRate5 {

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
      * 5、求movieid = 2116这部电影各年龄段（因为年龄就只有7个，就按这个7个分就好了）的平均影评（年龄段，影评分）
      *
      * 实现步骤：
      *
      *   1、首先要对电影进行过滤：  movieid = 2116
      *
      *   2、按照年龄作为key, 按照评分作为value
      */


    // (userid, (movieid, rate))
    val movie2116: RDD[(String, (String, String))] = rdd_rate.map(x => {
      val xx = x.split("::")
      val userid = xx(0)
      val movieid = xx(1)
      val rate = xx(2)
      (userid, (movieid, rate))
    }).filter(_._2._1 == "2116")

    val user_id_age: RDD[(String, String)] = rdd_user.map(x => {
      val xx = x.split("::")
      val userid = xx(0)
      val age = xx(2)
      (userid, age)
    })

    //  userid,   movieid,    rate,    age
    val user_rate_rdd: RDD[(String, ((String, String), String))] = movie2116.join(user_id_age)

    val age_rate_rdd: RDD[(String, Double)] = user_rate_rdd.map(x => {

      val age = x._2._2
      val rate = x._2._1._2.toInt

      (age, rate)
    }).groupByKey().map(x => {

      //      x    =====    (age, Iterable[Int])

      val age = x._1
      val sum = x._2.sum
      val size = x._2.toList.size
      val avgrate = sum * 1D / size

      (age, avgrate)
    }).sortByKey()


    age_rate_rdd.foreach(x => {
      println(x._1,  x._2)
    })

    sparkContext.stop()
  }
}
