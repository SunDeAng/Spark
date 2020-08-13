package com.atguigu.spark.project_streaming

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-19
 * @Desc:
 */
object Test02_HotByHour {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("chk")
    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "ads_log_0317"
    val group = "myGroup"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      //位置策略，指定计算的Executor
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams))

    //测试Kafka中消费数据
    val resDS: DStream[String] = kafkaDS.map(_.value())

    //定义窗口数据    因为测试：6秒相当于窗口大小1小时
    val windowDS: DStream[String] = resDS.window(Seconds(6),Seconds(3))

    val sdf = new SimpleDateFormat("hh:mm")

    //对从kafka中读取到的数据进行结构的转换  (av_hh:mm,1)
    val mapDS: DStream[(String, Int)] = windowDS.map(
      line => {
        val fields: Array[String] = line.split(",")
        (fields(4) + "_" + sdf.format(new Date(fields(0).toLong)), 1)
      }
    )

    mapDS.reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
