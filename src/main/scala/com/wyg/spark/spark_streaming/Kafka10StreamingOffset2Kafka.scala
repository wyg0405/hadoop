package com.wyg.spark.spark_streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Description:kafka 0.10
 * 在kafka记录偏移量，写到kafka自身
 * 参考
 * https://spark.apache.org/docs/2.3.3/streaming-kafka-0-10-integration.html
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-16 21:26
 * @version V1.0
 */
object Kafka10StreamingOffset2Kafka {

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

    val kafkaStream = KafkaUtils.createDirectStream(
      stream,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //offset相关
    kafkaStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(part => {
        //处理业务
        part.map(_.value()).foreach(println)
      })
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    stream.start()
    stream.awaitTermination()
  }
}
