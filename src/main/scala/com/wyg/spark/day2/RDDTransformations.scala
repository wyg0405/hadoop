package com.wyg.spark.day2

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.junit.{After, Before, Test}
import org.apache.spark.rdd.RDD


/**
 * Description: RDD API
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-03-23 14:00
 * @version V1.0
 */

class RDDAPI {

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


  @Test
  def map(): Unit = {
    arr.map((x: Int) => x * 2).foreach((x: Int) => println(x))
    text.foreach((x: String) => println(x))
  }

  /**
   * filter(func)
   * Return a new dataset formed by selecting those elements of the source on which func returns true.
   */
  @Test
  def filter(): Unit = {
    val filte1: RDD[String] = text.filter((x: String) => x.startsWith("hello"))
    println("===StartWith hello===")
    filte1.foreach((x: String) => println(x))
    println("===value>3===")
    arr.filter((x: Int) => x > 15).foreach((x: Int) => println(x))
    println("===偶数===")
    arr.filter((x: Int) => if (x % 2 == 0) true else false).foreach((x: Int) => println(x))
  }

  /**
   * flatMap(func)
   * Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item)
   */
  @Test
  def flatMap(): Unit = {
    println("===flatMap,Array===")
    //scala.TraversableOnce[U] 数组，集合，迭代器都可以
    text.flatMap((line: String) => line.split(" ")).foreach((word: String) => println(word))
    println("===flatMap,List===")
    text.flatMap((line: String) => line.split(" ").toList).foreach((word: String) => println(word))
  }

  /**
   * mapPartitions(func)
   * Similar to map, but runs separately on each partition (block) of the RDD, so func must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.
   */
  @Test
  def mapPartitions(): Unit = {
    println("===平方===")
    arr.mapPartitions((x: Iterator[Int]) => {
      println("===执行分区===")
      val list: List[Int] = x.toList.map((y: Int) => y * y)
      list.toIterator
    }).foreach((x: Int) => println(x))
  }

  /**
   * mapPartitionsWithIndex(func)
   * Similar to mapPartitions, but also provides func with an integer value representing the index of the partition, so func must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T.
   */
  @Test
  def mapPartitionsWithIndex(): Unit = {
    //f: (Int, scala.Iterator[T]) => scala.Iterator[U]，分区编号从0开始
    arr.mapPartitionsWithIndex((index, x) => {
      println("===执行分区为" + index + "===")
      val list: List[Int] = x.toList.map((y: Int) => y * y)
      list.toIterator
    }).foreach((x: Int) => println(x))
  }

  /**
   * sample(withReplacement, fraction, seed)
   * Sample a fraction fraction of the data, with or without replacement, using a given random number generator seed
   */
  @Test
  def sample(): Unit = {
    /**
     * def sample(withReplacement: Boolean,fraction: Double,seed: Long = {}): RDD[T]
     * withReplacement 是否放回
     * fraction 采样的概率和比率
     *
     */
    val range: Range.Inclusive = 1 to 1000
    val rdd: RDD[Int] = sc.parallelize(range)
    val unit: RDD[Int] = rdd.sample(false, 0.01, 0)
    unit.foreach((x: Int) => println(x))
  }

  /**
   * takeSample(withReplacement, num, [seed])
   * Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.
   */
  @Test
  def takeSample(): Unit = {
    val range: Range.Inclusive = 1 to 1000
    val rdd: RDD[Int] = sc.makeRDD(range)
    /**
     * def takeSample(withReplacement: Boolean,num: Int,seed: Long = {}): Array[T]
     * 按数量采样
     * sample返回RDD,takeSmaple返回Array
     *
     */
    val array: Array[Int] = rdd.takeSample(true, 10, 3)
    array.foreach((x: Int) => println(x))
  }


  /**
   * groupByKey([numPartitions])
   * When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
   * Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance.
   * Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numPartitions argument to set a different number of tasks.
   */
  @Test
  def groupByKey(): Unit = {
    val list: List[(String, Int)] = List(("math", 78), ("java", 88), ("scala", 90), ("math", 96), ("scala", 89), ("java", 92), ("math", 91), ("scala", 90))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list)

    val group: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val unit: Unit = group.foreach((x: (String, Iterable[Int])) => println(x))
    //每科平均值
    val avg: RDD[(String, Double)] = group.map((course: (String, Iterable[Int])) => {
      val avg: Double = course._2.sum.toDouble / course._2.size
      (course._1, avg)
    })
    println("====平均值===")
    avg.foreach(println(_: (String, Double)))
  }

  /**
   * reduceByKey(func, [numPartitions])
   * When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values
   * for each key are aggregated using the given reduce function func, which must be of
   * type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable
   * through an optional second argument.
   */
  @Test
  def reduceByKey(): Unit = {
    val flatMap: RDD[String] = text.flatMap(line => line.split(" "))
    flatMap.foreach(println(_))
    val map: RDD[(String, Int)] = flatMap.map(word => (word, 1))
    //map.foreach(println(_))
    val reduce: RDD[(String, Int)] = map.reduceByKey((x, y) => x + y).sortByKey()
    reduce.foreach(println(_))
  }

  @Test
  def sortBy(): Unit = {
    text.flatMap(line => line.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y).sortBy(x => x._2, false).foreach(println(_))
  }

  /**
   *
   */
  @Test
  def aggregate(): Unit = {
    /**
     * def aggregate[U](zeroValue: U)(seqOp: (U, T) => U,combOp: (U, U) => U)
     * zeroValue:初始值
     * seqOp:迭代操作，每个值都和初始值进行操作
     * combOp:分区结果合并
     */
    val sum: Int = course.aggregate(0)((x, y) => x + y._2, (x, y) => x + y)
    println("总和：" + sum)
    val tuple: (Int, Int) = course.aggregate((0, 0))((u: (Int, Int), x: (String, Int)) => (u._1 + x._2, u._2 + 1), (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2))
    val avg: Double = tuple._1.toDouble / tuple._2
    println("aggregate求平均值：" + avg)
    //fold求平均值
    //courseList.fold(())
    //foldLeft求平均值
    var foldLeft: (Int, Int) = courseList.foldLeft((0, 0))((x, y) => (x._1 + y._2, x._2 + 1))
    println("foldLeft求平均值：" + (foldLeft._1.toDouble / foldLeft._2))
  }

  /**
   * aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])
   * When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
   */
  @Test
  def aggregateByKey(): Unit = {

    /**
     * def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U)(implicit evidence$3: ClassTag[U]): RDD[(K, U)]
     * zeroValue:初始值
     * seqOp:迭代操作，每个值都和初始值进行操作
     * combOp:分区结果合并
     *
     * aggregateByKey=gruopByKey+aggregate
     *
     */
    val avg: RDD[(String, (Int, Int))] = course.aggregateByKey((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    println("===aggregateByKey求平均值===")
    avg.foreach(x => {
      println(x._1 + ":" + (x._2._1.toDouble / x._2._2))
    })
  }

  @Test
  def combineByKey(): Unit = {
    val avg: RDD[(String, (Int, Int))] = course.combineByKey((x: Int) => (x, 1), (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1), (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2))
    avg.foreach(x => {
      println(x._1 + ":" + (x._2._1.toDouble / x._2._2))
    })
  }

  /**
   * 全集
   * union(otherDataset)
   * Return a new dataset that contains the union of the elements in the source dataset and the argument.
   */
  @Test
  def union(): Unit = {
    val arr1: Array[Int] = Array(1, 2, 4, 6)
    val arr2: Array[Int] = Array(1, 3, 4, 5, 4)
    val rdd1: RDD[Int] = sc.makeRDD(arr1)
    val rdd2: RDD[Int] = sc.makeRDD(arr2)
    val union1: RDD[Int] = rdd1.union(rdd2)
    val union2: RDD[Int] = rdd2.union(rdd1)
    println("===rdd1.union(rdd2)===")
    union1.foreach(println(_))
    println("=== rdd2.union(rdd1)===")
    union2.foreach(println(_))

  }

  /**
   * 笛卡尔积
   * cartesian(otherDataset)
   * When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
   */
  @Test
  def cartesian(): Unit = {
    val arr1: Array[Int] = Array(1, 2, 4, 6)
    val arr2: Array[Int] = Array(1, 3, 4, 5, 4)
    val rdd1: RDD[Int] = sc.makeRDD(arr1)
    val rdd2: RDD[Int] = sc.makeRDD(arr2)
    println("===cartesian===")
    val cartesian1: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    val cartesian2: RDD[(Int, Int)] = rdd2.cartesian(rdd1)
    println("===rdd1.cartesian(rdd2)===")
    cartesian1.foreach(println(_))
    println("=== rdd2.cartesian(rdd1)===")
    cartesian2.foreach(println(_))
  }

  /**
   * 连接
   * join(otherDataset, [numPartitions])
   * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
   */
  @Test
  def join(): Unit = {
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((2, "e"), (3, "f"), (4, "g"), (5, "h"), (6, "i")))
    val join1: RDD[(Int, (Int, String))] = rdd1.join(rdd2)
    println("===rdd1.join(rdd2)===")
    join1.foreach(println(_))
    val join2: RDD[(Int, (String, Int))] = rdd2.join(rdd1)
    println("===rdd2.join(rdd1)===")
    join2.foreach(println(_))


  }

  @Test
  def leftOuterJoin(): Unit = {
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((2, "e"), (3, "f"), (4, "g"), (5, "h"), (6, "i")))
    val leftOuterJoin1: RDD[(Int, (Int, Option[String]))] = rdd1.leftOuterJoin(rdd2)
    println("===rdd1.leftOuterJoin(rdd2)===")
    leftOuterJoin1.foreach(println(_))
    val leftOuterJoin2: RDD[(Int, (String, Option[Int]))] = rdd2.leftOuterJoin(rdd1)
    println("===rdd2.leftOuterJoin(rdd1)===")
    leftOuterJoin2.foreach(println(_))
  }

  @Test
  def rightOuterJoin(): Unit = {
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((2, "e"), (3, "f"), (4, "g"), (5, "h"), (6, "i")))
    val right1: RDD[(Int, (Option[Int], String))] = rdd1.rightOuterJoin(rdd2)
    println("====rdd1.rightOuterJoin(rdd2)===")
    right1.foreach(println(_))
    val right2: RDD[(Int, (Option[String], Int))] = rdd2.rightOuterJoin(rdd1)
    println("====rdd2.rightOuterJoin(rdd1)===")
    right2.foreach(println(_))
  }

  @Test
  def fullOuterJoin(): Unit = {
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((2, "e"), (3, "f"), (4, "g"), (5, "h"), (6, "i")))
    val full1: RDD[(Int, (Option[Int], Option[String]))] = rdd1.fullOuterJoin(rdd2)
    println("===rdd1.fullOuterJoin(rdd2)===")
    full1.foreach(println(_))
    val full2: RDD[(Int, (Option[String], Option[Int]))] = rdd2.fullOuterJoin(rdd1)
    println("===rdd2.fullOuterJoin(rdd1)===")
    full2.foreach(println(_))
  }

  /**
   * 差集
   */
  @Test
  def subtract(): Unit = {
    val arr1: Array[Int] = Array(1, 2, 4, 6)
    val arr2: Array[Int] = Array(1, 3, 4, 5, 4)
    val rdd1: RDD[Int] = sc.makeRDD(arr1)
    val rdd2: RDD[Int] = sc.makeRDD(arr2)
    val subtrsct: RDD[Int] = rdd1.subtract(rdd2)
    println("===rdd1.subtract(rdd2)===")
    subtrsct.foreach(println(_))
    val subtrsct2: RDD[Int] = rdd2.subtract(rdd1)
    println("===rdd2.subtract(rdd1)===")
    subtrsct2.foreach(println(_))
  }

  /**
   * cogroup(otherDataset, [numPartitions])
   * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
   */
  @Test
  def cogruop(): Unit = {
    val rdd1: RDD[(Int, Int)] = sc.parallelize(Array((1, 1), (2, 2), (3, 3), (1, 3), (1, 3)))
    val rdd2: RDD[(Int, String)] = sc.parallelize(Array((2, "e"), (3, "f"), (3, "g"), (4, "h"), (1, "i")))
    rdd1.cogroup(rdd2).foreach(x => println(x._1, x._2._1.mkString("-"), x._2._2.mkString("-")))
  }

  /**
   * 交集
   * intersection(otherDataset)
   * Return a new RDD that contains the intersection of elements in the source dataset and the argument.
   */
  @Test
  def intersection(): Unit = {
    val arr1: Array[Int] = Array(1, 2, 4, 6)
    val arr2: Array[Int] = Array(1, 3, 4, 5, 4)
    val rdd1: RDD[Int] = sc.makeRDD(arr1)
    val rdd2: RDD[Int] = sc.makeRDD(arr2)
    val value: RDD[Int] = rdd1.intersection(rdd2)
    value.foreach(println)
  }

  @Test
  def partitionBy(): Unit = {
    val array: Array[(String, Int)] = Array(("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1), ("f", 1), ("g", 1), ("h", 1), ("i", 1), ("sdfaa", 1), ("auiu", 1))
    val value: RDD[(String, Int)] = sc.parallelize(array,4)
    value.mapPartitionsWithIndex((x,y)=>{
      println("分区："+x)
      y
    }).foreach(println)
    val part: RDD[(String, Int)] = value.partitionBy(new MyPartitioner(4))
    part.mapPartitionsWithIndex((x,y)=>{
      println("分区："+x)
      y
    }).foreach(println(_))
  }
}

class MyPartitioner(val numPtn: Int) extends Partitioner {
  /**
   * 分区数
   *
   * @return
   */
  override def numPartitions: Int = numPtn

  /**
   * 分区逻辑
   *
   * @param key
   * @return
   */
  override def getPartition(key: Any): Int = {
    val value: Int = (key.hashCode & Integer.MAX_VALUE )% numPtn
    value
  }
}
