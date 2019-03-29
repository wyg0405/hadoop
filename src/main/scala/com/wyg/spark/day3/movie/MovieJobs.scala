package com.wyg.spark.day3.movie

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * Description:
 * 数据描述
 *
 *
 * 现有如此三份数据：
 * 1、users.dat    数据格式为：  2::M::56::16::70072
 * 对应字段为：UserID BigInt, Gender String, Age Int, Occupation String, Zipcode String
 * 对应字段中文解释：用户id，性别，年龄，职业，邮政编码
 *
 * 2、movies.dat		数据格式为： 2::Jumanji (1995)::Adventure|Children's|Fantasy
 * 对应字段为：MovieID BigInt, Title String, Genres String
 * 对应字段中文解释：电影ID，电影名字，电影类型
 *
 * 3、ratings.dat		数据格式为：  1::1193::5::978300760
 * 对应字段为：UserID BigInt, MovieID BigInt, Rating Double, Timestamped String
 * 对应字段中文解释：用户ID，电影ID，评分，评分时间戳
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-26 20:06
 * @version V1.0
 */

class MovieJobs {

  var sp: SparkConf = null
  var sc: SparkContext = null
  var rddUser: RDD[String] = null
  var rddMovie: RDD[String] = null
  var rddRating: RDD[String] = null


  @Before
  def init(): Unit = {
    sp = new SparkConf()
    sp.setAppName("Rated most moive").setMaster("local[*]")
    sc = new SparkContext(sp)
    rddUser = sc.textFile("C:\\Users\\wyg04\\Desktop\\movie\\users.dat")
    rddMovie = sc.textFile("C:\\Users\\wyg04\\Desktop\\movie\\movies.dat")
    rddRating = sc.textFile("C:\\Users\\wyg04\\Desktop\\movie\\ratings.dat")
  }

  @After
  def destory: Unit = {
    sc.stop()
  }

  /**
   * 1、求被评分次数最多的10部电影，并给出评分次数（电影名，评分次数）
   */
  @Test
  def job1(): Unit = {
    val rating: RDD[(String, Int)] = rddRating.map(x => x.split("::")).map(x => (x(1), 1)).reduceByKey((x, y) => x + y)
    val top10: Array[(String, Int)] = rating.sortBy(x => x._2, false).take(10)
    val movie: RDD[(String, String)] = rddMovie.map(x => x.split("::")).map(x => (x(0), x(1)))
    val result: RDD[(String, Int)] = movie.join(sc.makeRDD(top10)).map(x => x._2).sortBy(x => x._2, false)
    println("===被评分次数最多的10部电影===")
    result.foreach(println)
  }

  /**
   * 2、分别求男性，女性当中评分最高的100部电影（性别，电影名，影评分）
   *
   */
  @Test
  def job2(): Unit = {
    val rating: RDD[(String, (String, Double))] = rddRating.map(x => x.split("::")).map(x => (x(0), (x(1), x(2).toDouble)))
    val movie: RDD[(String, String)] = rddMovie.map(x => x.split("::")).map(x => (x(0), x(1)))
    val user: RDD[(String, String)] = rddUser.map(x => x.split("::")).map(x => (x(0), x(1)))
    /**
     * 用户与影评关联并按（sex,movieId）为键聚合，得到性别，movieID,总分数，评价次数
     */
    val userRate: RDD[((String, String), (Double, Int))] = user.join(rating).map(x => ((x._2._1, x._2._2._1), x._2._2._2)).aggregateByKey((0.0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    //userRate.foreach(println)
    /**
     * 按（sex,movieId）为键,求得平均值,并与movie关联
     */
    val result: RDD[(String, String, Double)] = userRate.map(x => {
      (x._1._2, (x._1._1, x._2._1 / x._2._2))
    }).join(movie).map(x => (x._2._1._1, x._2._2, x._2._1._2))
    /**
     * 按性别筛选，取top100
     */
    val male: Array[(String, String, Double)] = result.filter(x => x._1 == "M").sortBy(x => x._3, false).take(100)
    val female: Array[(String, String, Double)] = result.filter(x => x._1 == "F").sortBy(x => x._3, false).take(100)
    println("男性评分最高的100部电影")
    male.foreach(println)
    println("女性评分最高的100部电影")
    female.foreach(println)
  }

  /**
   * 3、分别求男性，女性看过最多的10部电影（性别，电影名）
   */
  @Test
  def job3(): Unit = {
    val rating: RDD[(String, (String, Int))] = rddRating.map(x => x.split("::")).map(x => (x(0), (x(1), 1)))
    val movie: RDD[(String, String)] = rddMovie.map(x => x.split("::")).map(x => (x(0), x(1)))
    val user: RDD[(String, String)] = rddUser.map(x => x.split("::")).map(x => (x(0), x(1)))
    val userRate: RDD[((String, String), Int)] = user.join(rating).map(x => ((x._2._1, x._2._2._1), x._2._2._2)).reduceByKey((x, y) => x + y)
    //userRate.foreach(println(_))
    val result: RDD[(String, String, Int)] = userRate.map(x => (x._1._2, (x._1._1, x._2))).join(movie).map(x => (x._2._1._1, x._2._2, x._2._1._2))
    val male: Array[(String, String, Int)] = result.filter(x => x._1 == "M").sortBy(x => x._3, false).take(10)
    val female: Array[(String, String, Int)] = result.filter(x => x._1 == "F").sortBy(x => x._3, false).take(10)
    println("男性评分最高的10部电影")
    male.foreach(println)
    println("女性评分最高的10部电影")
    female.foreach(println)
  }

  /**
   * 4、年龄段在“18-24”的男人，最喜欢看10部电影
   */
  @Test
  def job4(): Unit = {
    val rating: RDD[(String, (String, Int))] = rddRating.map(x => x.split("::")).map(x => (x(0), (x(1), 1)))
    val movie: RDD[(String, String)] = rddMovie.map(x => x.split("::")).map(x => (x(0), x(1)))
    val user: RDD[(String, String, Int)] = rddUser.map(x => x.split("::")).map(x => (x(0), x(1), x(2).toInt))
    val result: Array[(Int, String)] = user.filter(x => (x._2 == "M") && (18 <= x._3) && (x._3 <= 24)).map(x => (x._1, x._2)).join(rating).map(x => (x._2._2._1, x._2._2._2)).reduceByKey((x, y) => x + y).join(movie).map(x => (x._2._1, x._2._2)).sortBy(x => x._1, false).take(10)
    println("年龄段在“18-24”的男人，最喜欢看10部电影")
    result.foreach(println)
  }

  /**
   * 5、求movieid = 2116这部电影各年龄段（因为年龄就只有7个，就按这个7个分就好了）的平均影评（年龄段，影评分）
   */
  @Test
  def job5(): Unit = {
    val rating: RDD[(String, Double)] = rddRating.map(x => x.split("::")).map(x => (x(0), x(1), x(2).toDouble)).filter(x => x._2 == "2116").map(x => (x._1, x._3))
    //val movie: RDD[ String] = rddMovie.map(x => x.split("::")).map(x => (x(0)).filter(x=>x=="2116"))
    val user: RDD[(String, Int)] = rddUser.map(x => x.split("::")).map(x => (x(0), x(2).toInt))
    val userRate: RDD[(Int, (Double, Int))] = user.join(rating).map(x => (x._2._1, x._2._2)).aggregateByKey((0.0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    val result: RDD[(Int, Double)] = userRate.map(x => (x._1, (x._2._1 / x._2._2))).sortByKey()
    result.foreach(println(_))

  }

  /**
   * 6、求最喜欢看电影（影评次数最多）的那位女性评最高分的10部电影的平均影评分（观影者，电影名，影评分）
   */
  @Test
  def job6(): Unit = {
    val rating: RDD[(String, Int)] = rddRating.map(x => x.split("::")).map(x => (x(0), 1))
    val movie: RDD[(String, String)] = rddMovie.map(x => x.split("::")).map(x => (x(0), x(1)))
    val user: RDD[(String, String)] = rddUser.map(x => x.split("::")).map(x => (x(0), x(1))).filter(x => x._2 == "F")
    val topUser: Array[(String, Int)] = user.join(rating).map(x => (x._1, x._2._2)).reduceByKey((x, y) => (x + y)).sortBy(x => x._2, false).take(1)
    val topMovie: RDD[(String, (String, Double))] = rddRating.map(x => x.split("::")).map(x => (x(1), (x(0), x(2).toDouble))).filter(x => x._2._1 == topUser(0)._1)
    val result: Array[(String, String, Double)] = topMovie.join(movie).map(x => (x._2._1._1, x._2._2, x._2._1._2)).sortBy(x => x._3, false).take(100)
    result.foreach(println(_))
  }

  /**
   * 7、求好片（评分>=4.0）最多的那个年份的最好看的10部电影
   */
  @Test
  def job7(): Unit = {
    val rating: RDD[(String, Double)] = rddRating.map(x => x.split("::")).map(x => (x(1), x(2).toDouble))
    val movie: RDD[(String, String)] = rddMovie.map(x => x.split("::")).map(x => (x(0), x(1)))
    val rateAvg: RDD[(String, Double)] = rating.aggregateByKey((0.0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, (x._2._1 / x._2._2)))

    val topYear: (String, Int) = movie.join(rateAvg).filter(x => x._2._2 >= 4.0).map(x => ({
      val start: Int = x._2._1.lastIndexOf("(")
      val end: Int = x._2._1.lastIndexOf(")")
      (x._2._1.substring(start + 1, end), 1)
    })).reduceByKey((x, y) => x + y).sortBy(x => x._2, false).first()

    val topMovies = movie.map(x => {
      val start: Int = x._2.lastIndexOf("(")
      val end: Int = x._2.lastIndexOf(")")
      (x._1, (x._2, x._2.substring(start + 1, end)))
    }).filter(x => x._2._2 == topYear._1).map(x => (x._1, x._2._1)).join(rateAvg).map(x => (x._2._1, x._2._2)).sortBy(x => x._2, false).take(100)
    topMovies.foreach(println)

  }

  /**
   * 8、求1997年上映的电影中，评分最高的10部Comedy类电影
   */
  @Test
  def job8(): Unit = {
    val movie = rddMovie.map(x => x.split("::")).map(x => {
      val year: String = x(1).substring(x(1).lastIndexOf("(") + 1, x(1).lastIndexOf(")"))
      //竖线需要转义才可以作为split的参数
      val style: Array[String] = x(2).toString().split("\\|")
      (x(0), x(1), year, style)
    }).filter(x => (x._3 == "1997")).filter(x => (x._4.contains("Comedy"))).map(x => (x._1, x._2))
    //movie.foreach(println(_))
    val rate: RDD[(String, Double)] = rddRating.map(x => x.split("::")).map(x => (x(1), x(2).toDouble))
    val topMovies: Array[(String, Double)] = movie.join(rate).aggregateByKey(("", 0.0, 0))((x, y) => (y._1, x._2 + y._2, x._3 + 1), (x, y) => (x._1, x._2 + y._2, x._3 + y._3)).map(x => {
      (x._2._1, (x._2._2 / x._2._3))
    }).sortBy(x => x._2, false).take(10)
    topMovies.foreach(println(_))
  }

  /**
   * 9、该影评库中各种类型电影中评价最高的5部电影（类型，电影名，平均影评分）
   */
  @Test
  def job9(): Unit = {
    //取出(movieId,Rating)
    val rate: RDD[(String, Double)] = rddRating.map(x => x.split("::")).map(x => (x(1), x(2).toDouble))
    //求平均评分
    val rateAvg: RDD[(String, Double)] = rate.aggregateByKey((0.0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, (x._2._1 / x._2._2)))

    //取出(movieId,(Title,Genres))
    val movie = rddMovie.map(x => x.split("::")).map(x => {
      //类型
      val genres: Array[String] = x(2).split("\\|")
      val builder = new StringBuilder
      genres.foreach(y => (builder.append(x(0)).append(::).append(x(1)).append("::").append(y)).append("|"))
      builder.substring(0, builder.length - 1)
    }).flatMap(x => x.split("\\|")).map(x => x.split("::")).map(x => (x(0), (x(1), x(2))))
    val unit: RDD[(String, Iterable[(String, Double)])] = movie.join(rateAvg).map(x => (x._2._1._2, (x._2._1._1, x._2._2))).groupByKey().sortByKey()

    println("各种类型电影中评价最高的5部电影（类型，电影名，平均影评分）")
    unit.collect().foreach(x => {
      val array: Array[(String, (String, Double))] = sc.makeRDD(x._2.toArray).sortBy(y => y._2, false).map(y => (x._1, y)).take(10)
      array.foreach(println)
    })

  }

  /**
   * 10、各年评分最高的电影类型（年份，类型，影评分）
   */
  @Test
  def job10(): Unit = {
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
    result.foreach(println)

  }
}
