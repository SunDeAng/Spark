package com.atguigu.spark.exer09

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-17
 * @Desc:
 */
object Exer04_Create_Kafka {

  def main(args: Array[String]): Unit = {

    //创建配置信息
    val conf = new SparkConf().setAppName("SparkSteaming").setMaster("local[*]")
    //常见SparkStreaming的入口StreamingContext
    //第二个参数为时间间隔
    val ssc = new StreamingContext(conf, Seconds(3))

    //构建Kafka参数
    val kafkaParmas: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop03:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "myGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //读取Kafka数据，创建DSteam
    val kafkaDS = KafkaUtils.createDirectStream(
      ssc, //传入ssc
      //存放数据策略，可以点进去查看详情
      LocationStrategies.PreferConsistent,
      //消费策略，传入订阅的主题，以及kafka配置信息
      //                           ????????????
      ConsumerStrategies.Subscribe[String, String](Set("bigdata0317"), kafkaParmas)
    )

    //处理数据
    kafkaDS
      .map(_.value())
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
