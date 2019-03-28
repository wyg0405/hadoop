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
object MovieRate2 {

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
      * 2、分别求男性，女性当中评分最高的10部电影（性别，电影名，影评分）
      *
      * 实现步骤：
      *
      *   1、求出男性/女性 评论的所有的电影的平均分
      *
      *       rdd_rate :  电影的ID
      *       rdd_user :  性别
      *       rdd_movie ： 电影名
      *
      *   2、按照计算出来的  电影平均分进行降序排序， 取 前10
      */

    val rate_rdd: RDD[(String, (String, String))] = rdd_rate.map(x => {
      val xx = x.split("::")
      // 用户ID， 电影ID，评分
      (xx(0), (xx(1),xx(2)))
    })

    val user_rdd: RDD[(String, String)] = rdd_user.map(x => {
      val xx = x.split("::")
      (xx(0), xx(1))
    })

    // (String, (String, String))
    //   用户ID，  电影ID， 评分，   性别
    val sex_rate_rdd: RDD[(String, ((String, String), String))] = rate_rdd.join(user_rdd).filter(_._2._2 == "M")

    val movie_id_rate: RDD[(String, Int)] = sex_rate_rdd.map(x => {
      val movieid = x._2._1._1
      val rate = x._2._1._2.toInt
      (movieid, rate)
    })


    /**
      * 执行之前： huangbo	33
                  huangbo	44
                  huangbo	55
                  xuzheng	55
                  xuzheng	66
      */
    val rdd = movie_id_rate.groupByKey()

    /**
      * 执行之后：
      * huangbo		33,44,55     ----->    huangbo  44
        *xuzheng		55,66
 **
 *
 x  ==  (112,   [5,4,2,6,4,1])
 *
 */
    val movie_avgrate: RDD[(String, Double)] = rdd.map(x => {

      val movieid = x._1

      val values: Iterable[Int] = x._2
      val sum = values.sum
      val size = values.toList.size

      val avgrate = sum * 1D / size

      (movieid, (avgrate,sum))
    }).filter(x => {
      x._2._2 >= 50
    }).map(x => {
      (x._1, x._2._1)
    }).sortBy(_._2, false)


    val top10: Array[(String, Double)] = movie_avgrate.take(10)


    val movie_id_name: RDD[(String, String)] = rdd_movie.map(x => {
      val xx = x.split("::")
      (xx(0), xx(1))
    })

    // 电影ID，  电影的名称，  电影的评分
    val result: RDD[(String, (String, Double))] = movie_id_name.join(sparkContext.makeRDD(top10))

    result.foreach(x => {
      println(x._1, x._2._1, x._2._2)
    })

    sparkContext.stop()
  }
}
