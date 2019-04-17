package com.wyg.redis

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * Description: 
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-17 13:56
 * @version V1.0
 */

object RedisPool {
  val conf = new JedisPoolConfig()
  //最大连接数
  conf.setMaxTotal(20)
  //空闲最大连接数
  conf.setMaxIdle(10)
  //有效性检查
  conf.setTestOnBorrow(true)

  //配置连接
  val pool = new JedisPool(conf, "hadoop1", 6379, 10000, "eternal.9")

  def getConnection = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = RedisPool.getConnection
    val stu1 = conn.get("tom")
    println(stu1)
    conn.incrBy("tom", 10)
    val stu2 = conn.get("tom")
    println(stu2)
    conn.close()
  }
}
