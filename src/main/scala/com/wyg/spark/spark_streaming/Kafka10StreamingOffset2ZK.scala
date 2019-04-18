package com.wyg.spark.spark_streaming

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
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
 * 在kafka记录偏移量，写到zk
 * 参考
 * https://spark.apache.org/docs/2.3.3/streaming-kafka-0-10-integration.html
 *
 * @author: wyg0405@gmail.com
 * @date: 2019-04-16 21:26
 * @version V1.0
 */
object Kafka10StreamingOffset2ZK {

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
    val zk = "hadoop1:2181,hadoop2:2181,hadoop3:2181,hadoop4:2181"

    //zk中写入数据的目录，用于保存偏移量
    val topicDir = new ZKGroupTopicDirs(topic, group)
    //获取 zookeeper 中的路径 "/g001/offsets/topic1/"
    val zkTopicPath = s"${topicDir.consumerOffsetDir}"

    //zookeeper 的host 和 ip，创建一个 client,用于跟新偏移量量的
    //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zk)
    val children = zkClient.countChildren(zkTopicPath)
    var offsetsMap: Map[TopicPartition, Long] = Map()
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    if (children > 0) {
      //有分区数据
      for (i <- 0 until (children)) {
        val partitionOffset: String = zkClient.readData[String](zkTopicPath + "/" + i)
        val topicPartition = new TopicPartition(topic, i)
        offsetsMap += (topicPartition -> partitionOffset.toLong)
      }
      kafkaStream = KafkaUtils.createDirectStream(
        stream,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](offsetsMap.keys.toList, kafkaParams, offsetsMap)
      )
    } else {
      //无分区数据
      kafkaStream = KafkaUtils.createDirectStream(
        stream,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    }

    //直连方式只有在KafkaDStream的RDD中才能获取偏移量，那么就不能到调用DStream的Transformation
    //所以只能子在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
    kafkaStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(part => {
        //业务处理
        part.map(_.value()).foreach(println)

        //更新offset
        val client = new ZkClient(zk)
        //获取offset相关信息
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        val offsetDir = zkTopicPath + "/" + o.partition
        //ZkUtils.updatePersistentPath(zkClient, offsetDir, o.untilOffset.toString)
        ZkUtils(client, false).updatePersistentPath(offsetDir, o.untilOffset.toString)

      })

    }

    stream.start()
    stream.awaitTermination()
  }
}
