package com.wyg.spark.spark_streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Description:
 * 参考
 * https://spark.apache.org/docs/2.3.3/streaming-kafka-0-10-integration.html
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-16 21:26
 * @version V1.0
 */

object KafkaStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafka Streaming Word Count").setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(5))
    val kafkaParams = Map(
      "metadata.broker.list" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "topic1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("topic1");
    //可添加多个
    val result: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      stream,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    result.map(record => (record.key, record.value))
  }

}
