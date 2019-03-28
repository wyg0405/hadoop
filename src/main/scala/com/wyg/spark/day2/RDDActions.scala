package com.wyg.spark.day2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.{After, Before, Test}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-25 22:12
 * @version V1.0
 */

class RDDActions {
  var conf: SparkConf = null
  var sc: SparkContext = null
  var text: RDD[String] = null
  var range: Range.Inclusive = 1 to 20
  var arr: RDD[Int] = null
  var courseList: List[(String, Int)] = null
  var course: RDD[(String, Int)] = null

  /**
   * 初始化
   */
  @Before
  def void(): Unit = {
    println("==========初始化：开始================")
    conf = new SparkConf()
    conf.setAppName("RDDAPI").setMaster("local")
    sc = new SparkContext(conf)
    text = sc.textFile("C:\\Users\\wyg04\\Desktop\\word.txt")
    courseList = List(("math", 78), ("java", 88), ("scala", 90), ("math", 96), ("scala", 89), ("java", 92), ("math", 91), ("scala", 90))
    //3个分区
    arr = sc.parallelize(range, 3)
    course = sc.makeRDD(courseList)

    println("==========初始化：结束================")
  }

  /**
   * 释放资源
   */
  @After
  def destory(): Unit = {
    println("===========结束，销毁资源===========")
    sc.stop()
  }

  /**
   * reduce(func)
   * Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.
   */
  @Test
  def reduceByKeyLocally: Unit = {
    println("===reduceByKey===")
    val reduceByKey: RDD[(String, Int)] = course.reduceByKey((x, y) => x + y)
    reduceByKey.foreach(println)
    println("===reduceByKeyLocally===")
    val reduceByKeyLocally: collection.Map[String, Int] = course.reduceByKeyLocally((x, y) => x + y)
    reduceByKeyLocally.foreach(println)

  }


  @Test
  def collect(): Unit ={
    /**
     * 把所有分区的数据收集到客户端
     * collect()
     * Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.
     */
    println("===collect===")
    val collect: Array[Int] = arr.collect()
    collect.foreach(x=>print(x+" "))

    /**
     * count()
     * Return the number of elements in the dataset.
     */
    println("===count===")
    val count: Long = arr.count()
    println("rdd长度："+count)

    /**
     * first()
     * Return the first element of the dataset (similar to take(1)).
     */
    println("===first===")
    val first: Int = arr.first()
    println("first data:"+first)

    /**
     * 不对数据进行排序，返回rdd 中从0到N 的下标表示的值
     * take(n)
     * Return an array with the first n elements of the dataset.
     */
    val take: Array[(String,Int)] = course.take(5)
    println("===take 5 record====")
    take.foreach(x=>print(x+" "))

    /**
     * 普通数据类型会自动向上转型
     * takeOrdered(n, [ordering])
     * Return the first n elements of the RDD using either their natural order or a custom comparator
     */
    val takeOrdered: Array[(String, Int)] = course.takeOrdered(5)
    println("===takeOrdered 5 record====")
    takeOrdered.foreach(x=>print(x+" "))
    println("===top 5 record====")
    course.top(5).foreach(println(_))
  }

}
