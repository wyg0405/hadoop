package com.wyg.spark.spark_streaming

import kafka.utils.ZKGroupTopicDirs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Description: 直连方式
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-17 9:07
 * @version V1.0
 */

object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Duration(5000))
    //topic
    val topic = "wordcount"
    //group
    val group = "g1"
    //brokerlist
    val brokelist = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092"
    //zk，记录和更新偏移量，也可以用mysql，redis等
    val zk = "hadoop1:2181,hadoop2:2181,hadoop3:2181,hadoop4:2181"
    val topics = Set(topic)
    //ZKGroupTopicDirs,用于保存偏移量的目录
    val topicDir = new ZKGroupTopicDirs(group, topic)
    //获取zk中偏移量地址
    val zkTopicPath = s"${topicDir.consumerOffsetDir}"

    //kafkaParams
    val kafkaParams = Map(
      "metadata.broker.list" -> brokelist,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // val zkClient=new ZkClient()

  }
}
