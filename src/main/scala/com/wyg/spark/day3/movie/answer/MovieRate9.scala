package com.wyg.spark.day3.movie.answer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

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
object MovieRate9 {

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
      * 9、该影评库中各种类型电影中评价最高的5部电影（类型，电影名，平均影评分）
      *
      *     电影类型           电影ID                         电影的评分
      *     movie.type     movie.id, rate.movieid             rate.rate
      *
      *     a,  aa1, 4.99
      *     a,  aa2, 4.98
      *     ...
      *     b,  aa1, 4.99
      *     b,  bb2, 4
      *
      *     aa1     4.99      a|b
      *     3829    name      Comedy|Romance
      *
      *     3829    name      Comedy
      *     3829    name      Romance
      *
      *     如果能得到以上数据，。 那么只需要先按照 电影类型 分组，
      *     每组数据， 按照电影评分排降序
      *     然后每组数据中取5条
      *
      * 实现思路：
      *
      *   1、  获取 一个 RDD ===== movie_type_id_avgrate_rdd  (type, movieid,  avgrate)
      *
      *                                         rdd_movie:   movieid,   type
      *                                         rdd_rate:    movieid,   rate
      *
      *                                         对rate按照movieid分组之后，求平均值
      *
      *                                         对type进行拆分
      *
      *   2、  按 type 分组，  按电影评分排降序 ，  每组取5条记录
      */


    val movie_id_type: RDD[(String, String)] = rdd_movie.map(x => {
      val xx = x.split("::")
      val movieid = xx(0)
      val movietype = xx(2)
      (movieid, movietype)
    })


    val movie_id_rate: RDD[(String, Int)] = rdd_rate.map(x => {
      val xx = x.split("::")
      val movieid = xx(1)
      val rate = xx(2)
      (movieid, rate.toInt)
    })

    //                           电影ID，   电影Type，  评分
    val movie_id_type_rate: RDD[(String, (String, Int))] = movie_id_type.join(movie_id_rate)

    /**
      * (String, String, Double)  ==== ID， Type， AvgRate
      */
    val movie_id_type_avgrate_rdd_last: RDD[(String, String, Double)] = movie_id_type_rate.groupByKey().map(x => {

      // x ==== (String, Iterable[(String, Int)])
      val movieid = x._1

      var movietype = ""
      val iterator = x._2.iterator

      var sum = 0
      var count = 0

      while (iterator.hasNext) {
        val tuple = iterator.next() // (String, Int)

        sum += tuple._2
        count += 1

        movietype = tuple._1
      }

      val avgrate = sum * 1D / count


      //  123,   a|b|c      4.55

      //  123    a    4.55
      //  123    b    4.55
      //  123    c    4.55
      (movieid, movietype, avgrate)
    }).flatMap(x => {

      // x ===   (String, String, Double)
      val ab: ArrayBuffer[(String, String, Double)] = new ArrayBuffer[(String, String, Double)]()

      val types: Array[String] = x._2.split("\\|")

      for (type_c <- types) {

        ab.append((x._1, type_c, x._3))

      }
      ab.toArray.iterator
    }).sortBy(_._1, false)


    /**
      * (String, String, Double)
      * 电影ID， 电影类型， 电影评分
      */
//    movie_id_type_avgrate_rdd_last.foreach(x => {
//      println(x._1, x._2, x._3)
//    })


    /**
      * (String, String, Double)
      * 电影类型， 电影ID， 电影评分
      */
    val last_RDD: RDD[(String, String, Double)] = movie_id_type_avgrate_rdd_last.map(x => {

      // 电影的类型， （电影的ID， 电影的评分）
      (x._2, (x._1, x._3))
    }).groupByKey().flatMap(x => {

      // x ==== (String, Iterable[(String, Double)])

      /**
        * 到了这一步， 我只想取 这个 Iterable 中的  按照 评分排降序之后  的 前五个信息
        */
      val tuples: List[(String, Double)] = x._2.toList.sortWith((xx, yy) => xx._2 > yy._2)

      /**
        * 如果这个List的长度不足5呢？      有几个取几个！！！！！
        */
      var length = if (tuples.length > 5) 5 else tuples.length

      //  存储的元素就是：  (String, Double)  === 电影的ID，  电影的评分，
      //  电影的类型！！！！ 没有
      val aabb: ArrayBuffer[(String, String, Double)] = new ArrayBuffer[(String, String, Double)]()

      var counter = 1
      while (counter <= length) {

        val unit_element = tuples(counter - 1)

        // 最终的 RDD 中的  元素的  样子！！！！！
        // 电影的类型，   电影的ID，  电影的评分
        val element = (x._1, unit_element._1, unit_element._2)

        aabb.append(element)

        counter += 1
      }

      // 电影类型， 电影ID，  电影评分
      aabb.iterator

    }).sortBy(_._1, false)

    last_RDD.foreach(x => {
      println(x._1, x._2, x._3)
    })

    sparkContext.stop()
  }
}
