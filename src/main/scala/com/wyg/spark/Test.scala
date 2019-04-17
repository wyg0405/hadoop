package com.wyg.spark

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-12 12:41
 * @version V1.0
 */

object Test {
  def main(args: Array[String]): Unit = {

    val a = "1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302"
    val b = a.split("\\|")
    b.foreach(println)

    println(ip2Long("124.123.45.67"))

    val c = "http://bigdata.edu360.cn/laozhang"
    println(c.substring(c.lastIndexOf("/") + 1))
    println(c.substring(c.indexOf("//") + 2, c.indexOf(".")))
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
