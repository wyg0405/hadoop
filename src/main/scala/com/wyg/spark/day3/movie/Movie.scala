package com.wyg.spark.day3.movie

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-28 11:55
 * @version V1.0
 */

object Movie {
  var sp: SparkConf = null
  var sc: SparkContext = null
  var rddUser: RDD[String] = null
  var rddMovie: RDD[String] = null
  var rddRating: RDD[String] = null

  def main(args: Array[String]): Unit = {
    sp = new SparkConf()
    sp.setAppName("Rated most moive")
    sc = new SparkContext(sp)
    rddUser = sc.textFile("/input/movie/users.dat")
    rddMovie = sc.textFile("/input/movie/movies.dat")
    rddRating = sc.textFile("/input/movie/ratings.dat")
    //取出(movieId,Rating)
    val rate: RDD[(String, Double)] = rddRating.map(x => x.split("::")).map(x => (x(1), x(2).toDouble))

    val movie = rddMovie.map(x => x.split("::")).map(x => {
      val year: String = x(1).substring(x(1).lastIndexOf("(") + 1, x(1).lastIndexOf(")"))
      //竖线需要转义才可以作为split的参数
      val style: Array[String] = x(2).toString().split("\\|")
      (x(0), (year, style))
    }).join(rate).map(x => (x._2._1._1, (x._2._1._2, x._2._2))).groupByKey().sortByKey()

    var result: ArrayBuffer[(String, String, Double)] = new ArrayBuffer[(String, String, Double)]();
    movie.collect.foreach(x => {
      val year: RDD[(Array[String], Double)] = sc.makeRDD(x._2.toArray)

      val value = year.map(y => {
        val builder = new StringBuilder
        y._1.foreach(z => {
          (builder.append(z).append("::").append(y._2).append("|"))
        })
        (builder.substring(0, builder.length - 1))
      }).flatMap(y => y.split("\\|")).map(y => y.split("::")).map(y => (y(0), y(1).toDouble)).aggregateByKey((0.0, 0))((a, b) => (a._1 + b, a._2 + 1), (a, b) => (a._1 + b._1, a._2 + b._2
      )).map(y => (y._1, (y._2._1 / y._2._2))).sortBy(y => y._2, false)
      result.append(value.map(y => (x._1, y._1, y._2)).first())
    })
    sc.makeRDD(result).saveAsTextFile(args(0))
  }
}
