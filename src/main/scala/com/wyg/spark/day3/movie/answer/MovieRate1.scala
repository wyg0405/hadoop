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
object MovieRate1 {

  def main(args: Array[String]): Unit = {

    /**
      * 初始化SparkContext
      */
    val sparkConf = new SparkConf().setAppName("MoDEL").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val rdd_movie: RDD[String] = sparkContext.textFile("D:\\bigdata\\movie\\movies.dat")
    val rdd_rate: RDD[String] = sparkContext.textFile("D:\\bigdata\\rate\\ratings.dat")


    /**
      * 实现需求：
      * 1、求被评分次数最多的10部电影，并给出评分次数（电影名，评分次数）
      *
      * 需要做的事情：
      *   1、求出所有电影的评分次数
      *
      *   2、对这些电影按照 评分次数 降序排序
      *
      *   3、去排序之后的前10条结果
      *
      *
      *   结果样式：
      *
      *   movie1, 888
      *   movie2, 555
      *   .....
      */

    // (String, Int)  ----->   电影的ID，  被评论了一次
    val movie_rate_one: RDD[(String, Int)] = rdd_rate.map((line: String) => {

      val fields: Array[String] = line.split("::")

      val movieid = fields(1)
      val value = 1

      (movieid, value)
    })


    val movie_counts: RDD[(String, Int)] = movie_rate_one.reduceByKey(_+_)


//    val sorted_rdd: RDD[(String, Int)] = movie_counts.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    val sorted_rdd: RDD[(String, Int)] = movie_counts.sortBy( (t:(String, Int)) => t._2,  false)


    val top10: Array[(String, Int)] = sorted_rdd.take(10)

//    top10.foreach(x => println(x._1, x._2))


    // x ==== 5::Father of the Bride Part II (1995)::Comedy
    val movie_id_name_rdd: RDD[(String, String)] = rdd_movie.map(x => {

      val xx = x.split("::")

      (xx(0), xx(1))
    })

    //  电影ID， 电影name， 被评论次数
    val result_rdd: RDD[(String, (String, Int))] = movie_id_name_rdd.join(sparkContext.makeRDD(top10))
      .sortBy(_._2._2, false)


    // x  ===== (String, (String, Int))
    result_rdd.foreach(x => {

      println(x._1, x._2._1, x._2._2)
    })

    sparkContext.stop()
  }
}
