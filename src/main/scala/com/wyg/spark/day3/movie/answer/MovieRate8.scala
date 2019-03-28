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
object MovieRate8 {

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
      * 8、求1997年上映的电影中，评分最高的10部Comedy类电影
      *
      * 实现思路：
      *
      *   1、val movie_id_year_rate_rdd: RDD[(String, String, Double)]
      *                                        电影ID， 电影年份，  评分
      *
      *      val movie_id_year_rate_rdd: RDD[(String, String, Double, String)]
      *                                        电影ID， 电影年份，  评分   类型
      *
      *
      *                                        (movieid, year, movie_type, avgrate)
      *
      *    2、    1997过滤     comedy过滤      然后按照avgrate降序排序， 然后取前10
      */

    val movie_id_rate: RDD[(String, Int)] = rdd_rate.map(x => {
      val xx:Array[String] = x.split("::")
      val movieid = xx(1)
      val rate = xx(2).toInt
      (movieid, rate)
    })

    val movie_id_year: RDD[(String, (String, String))] = rdd_movie.map(x => {
      val xx = x.split("::")
      val movieid = xx(0)
      val movie_name = xx(1)
      val movie_type = xx(2)
      val year = movie_name.substring(movie_name.length - 5,   movie_name.length - 1)
      (movieid,  (year, movie_type))
    })

    // (String,  ((String, String), Int))
    // (String, ((String, String), Int))
  val movie_id_year_rate_rdd_temp: RDD[(String, ((String, String), Int))] = movie_id_year.join(movie_id_rate)

    val movie_id_year_rate_rdd: RDD[(String, String, String, Double)] = movie_id_year_rate_rdd_temp.groupByKey().map(x => {

      // 电影ID，       年份      类型      评分
      // x =   (String, Iterable[((String, String), Int))])

      val iter: Iterable[((String, String), Int)] = x._2
      val movieid = x._1

      val iterator = iter.iterator

      var sum = 0
      var count = 0
      var year = ""
      var movie_type = ""

      while (iterator.hasNext) {
        val tuple: ((String, String), Int) = iterator.next()

        sum += tuple._2
        count += 1

        year = tuple._1._1
        movie_type = tuple._1._2
      }

      val avgrate = sum * 1D / count


      (movieid, year, movie_type, avgrate)
    })


    /**
      *
      *  (movieid, year, movie_type, avgrate)
      *
      *  1997
      *
      *  comedy
      */
    val movie_1997_comedy_rdd: RDD[(String, String, String, Double)] = movie_id_year_rate_rdd.filter(_._2 == "1997")
      .filter(_._3.toLowerCase.indexOf("comedy") != -1)

    val most_top10_comedy_1997: Array[(String, String, String, Double)] = movie_1997_comedy_rdd.sortBy(_._4, false).take(10)


    most_top10_comedy_1997.foreach(x => {
      println(x._1, x._2, x._3, x._4)
    })


    sparkContext.stop()
  }
}
