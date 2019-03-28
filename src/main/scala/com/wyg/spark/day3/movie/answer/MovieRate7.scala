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
object MovieRate7 {

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
      * 7、求好片（评分>=4.0）最多的那个年份的最好看的10部电影
      *
      * 实现思路：
      *
      *   1、好片  这部电影的评分如果是 >= 4.0  就是好片
      *
      *     所有的电影，都有一个上映年份。 上映年份没有一个独立的字段，是夹杂在 电影名称中。！！！！
      *
      *   2、最终，要按照年份分组，求出每一年中，好片的数量，然后按照数量降序排序，取第一个，就是得到好片数量最多的年份了。！！！
      *
      *   3、求这一年中，最好看的10个电影。！！
      *
      *       1、假如所有年份中的， 每一年好片的数量都没有超过10，
      *       那么最终的结果 到底显示什么？  （1998     20     6）   输出 10  部电影！！！
      */

    /**
      * 第一步，求出三个信息  movie_id_year_rate_rdd ：  （movieid,  year,  avgrate）
      *                                                  电影ID，   年份，    评分
      */
    val movie_id_rate: RDD[(String, Int)] = rdd_rate.map(x => {
      val xx:Array[String] = x.split("::")
      val movieid = xx(1)
      val rate = xx(2).toInt
      (movieid, rate)
    })

    val movie_id_year: RDD[(String, String)] = rdd_movie.map(x => {
      val xx = x.split("::")
      val movieid = xx(0)
      val movie_name = xx(1)
      val year = movie_name.substring(movie_name.length - 5,   movie_name.length - 1)
      (movieid,  year)
    })

    val movie_id_year_rate_rdd: RDD[(String, String, Double)] = movie_id_year.join(movie_id_rate)
      .groupByKey().map(x => {

      // x  ==== (String, Iterable[(String, Int)])
      val iterator = x._2.iterator

      var sum = 0
      var count = 0
      var year:String = ""

      while(iterator.hasNext){
        val tuple = iterator.next()
        sum += tuple._2
        count += 1
        year = tuple._1
      }

      val avgrate = sum * 1D / count

      (x._1,  year,  avgrate)
    }).cache()


    /**
      * 第二部：  先进行好片过滤，然后按照年份分组， 统计每一年中的好片数量， 然后降序排序，取第一条记录，就是好片最多的年份
      */
    val most_year:String = movie_id_year_rate_rdd.filter(x => {
      x._3 >= 4.0
    }).map(x => {
      (x._2, 1)
    }).reduceByKey(_+_)    //  (String, Int)   年份， 好片数据量
      .sortBy(_._2, false).first()._1


    /**
      * 第三步： 根据 第二步  求得 好片，  再在movie_id_year_rate_rdd按照这个年份进行过滤
      *
      *     过滤之后，就只剩下了该年中的所有电影。！！！   按照这些电影的 avgrate 字段降序排序，取前10
      */

    //  movieid,   year,   avgrate
    val most_top10: Array[(String, String, Double)] = movie_id_year_rate_rdd.filter(x => {
      x._2 == most_year
    }).sortBy(_._3, false).take(10)

    most_top10.foreach(x => {
      println(x._1, x._2, x._3)
    })

    sparkContext.stop()
  }
}
