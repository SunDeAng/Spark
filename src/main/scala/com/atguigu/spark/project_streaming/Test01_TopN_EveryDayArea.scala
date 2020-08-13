package com.atguigu.spark.project_streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-18
 * @Desc:
 */
object Test01_TopN_EveryDayArea {

  def main(args: Array[String]): Unit = {

    //创建DataStreaming
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))

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


    //7.2	每天每地区热门广告Top3
    //获取kafka中的每一行数据
    val lineValueDS: DStream[String] = kafkaDS.map(_.value())

    //  line => ((时间-地区-广告id),1)
    val mapDS: DStream[(String, Int)] = lineValueDS.map {
      line => {
        val fields: Array[String] = line.split(",")
        val timeStamp = fields(0).toLong
        val day = new Date(timeStamp)
        val fm = new SimpleDateFormat("yyyy-MM-dd")
        val dayStr = fm.format(day)
        val area = fields(1)
        val adv = fields(4)
        ((dayStr + "-" + area + "-" + adv), 1)
      }
    }

    val updateDS = mapDS.updateStateByKey {
      (seq: Seq[Int], buffer: Option[Int]) => {
        Option(buffer.getOrElse(0) + seq.sum)
      }
    }

    //((时间-地区-广告id),count)=>((时间-地区),(广告id,count))
    val map1DS = updateDS.map {
      case (k, count) => {
        val array = k.split("-")
        (array(0) + "-" + array(1), (array(2), count))
      }
    }

    //分组
    val groupDS: DStream[(String, Iterable[(String, Int)])] = map1DS.groupByKey()

    //排序取前三
    val resDS = groupDS.mapValues(
      data => {
        data.toList.sortBy(-_._2).take(3)
      }
    )

    resDS.print

    //启动消费
    ssc.start()
    ssc.awaitTermination()
  }

}


