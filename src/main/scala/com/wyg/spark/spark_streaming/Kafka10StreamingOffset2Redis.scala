package com.wyg.spark.spark_streaming

import java.util

import com.wyg.redis.RedisPool
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


/**
 * Description:kafka 0.10
 * 在kafka记录偏移量，写到redis
 * 参考
 * https://spark.apache.org/docs/2.3.3/streaming-kafka-0-10-integration.html
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-16 21:26
 * @version V1.0
 */
object Kafka10StreamingOffset2Redis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Kafka Streaming Word Count")
      .setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(10))
    val topic = "topic1"
    val topics = Array(topic);
    val group = "g1"
    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092"
    //构建kafka参数
    val kafkaParams = Map(
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //如果要使用更新历史数据（累加），那么就要把终结结果保存起来
    stream.checkpoint("./checkpoint")
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null


    //构造offset键。topic_group_partiton,只考虑一个topic
    val offsetKey = topic + "_" + group + "_"
    val conn = RedisPool.getConnection
    //多个分区
    val keys: util.Set[String] = conn.keys(offsetKey + "*")
    var fromOffsets: Map[TopicPartition, Long] = Map()
    //已有分区数据
    if (!keys.isEmpty) {
      //
      val iterator = keys.iterator()
      while (iterator.hasNext) {

        val part: String = iterator.next()
        var lastOffset = 0L
        val lastSavedOffset = conn.get(part)

        if (null != lastSavedOffset) {
          try {
            lastOffset = lastSavedOffset.toLong
          } catch {
            case ex: Exception =>
              println(ex.getMessage)
              println(
                "get lastSavedOffset error, lastSavedOffset from redis [" + lastSavedOffset + "] ")
              System.exit(1)
          }
        }
        //设置每个分区起始的Offset
        val partNum = part.substring(part.lastIndexOf("_") + 1).toInt
        fromOffsets += (new TopicPartition(topic, partNum) -> lastOffset)
      }
      kafkaStream = KafkaUtils.createDirectStream(
        stream,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )

    } else {
      //无分区数据
      kafkaStream = KafkaUtils.createDirectStream(
        stream,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    }

    //offset相关
    kafkaStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(part => {
        part.map(_.value()).foreach(println)
        //获取offset相关信息
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

        //更新offset
        val jedis = RedisPool.getConnection
        jedis.set(offsetKey + o.partition, o.untilOffset.toString)
        jedis.close()
      })
    }

    stream.start()
    stream.awaitTermination()
  }
}
