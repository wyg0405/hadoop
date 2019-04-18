package com.wyg.spark.spark_streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
 * Description:kafka 0.10
 * 参考
 * https://spark.apache.org/docs/2.3.3/streaming-kafka-0-10-integration.html
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-16 21:26
 * @version V1.0
 */

object Kafka10StreamingWordCount {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafka Streaming Word Count").setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(5))
    val kafkaParams = Map(
      "bootstrap.servers" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "topic1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("topic1");

    stream.checkpoint("./checkpoint")
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      stream,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //kafkaStream.map(record => (record.key(),record.value())).print()

    val wordAndOne: DStream[(String, Int)] = kafkaStream.map(_.value()).flatMap(line => line.split(" ")).map((_, 1))
    /*val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    result.print()*/
    //实现可累加的Wordcount
    /**
     * http://blog.leanote.com/post/kobeliuziyang/SparkStreaming%E7%8A%B6%E6%80%81%E7%AE%A1%E7%90%86
     * 在updateStateByKey算子中，有效率低下的问题，mapWithState解决了该问题并且添加了两个新的功能。 核心思路是创建一个新的MapWithStateRDD，该RDD的元素是MapWithStateRDDRecord，每个MapWithStateRDDRecord记录了某个partition下所有的key的State。
     */

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      state.update(sum)
      (word, sum)
    }

    val result2 = wordAndOne.mapWithState(StateSpec.function(mappingFunc))
    result2.stateSnapshots().print()
    stream.start()
    stream.awaitTermination()
  }
}
