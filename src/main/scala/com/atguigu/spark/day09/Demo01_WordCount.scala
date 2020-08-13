package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-17
 * @Desc: SparkStreaming入门案例
 */
object Demo01_WordCount {

  def main(args: Array[String]): Unit = {
    //配置信息
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //创建程序执行入口
    val ssc = new StreamingContext(conf,Seconds(3))


    //读取指定端口获取数据，并且数据以流形式源源不断传输
    val lineDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //对当前读到的一行数据进行扁平化映射
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))
    //对flatMap数据进行结构转换
    val mapDS = flatMapDS.map((_, 1))
    //对数据进行计数
    val reduceDS = mapDS.reduceByKey(_ + _)

    //行动算子
    reduceDS.print()

    //开始采集
    ssc.start()

    //释放资源    因为要实时不间断采集数据，所有现在不能调用stop方法结束
    //ssc.stop()
    //让采集线程一直执行
    ssc.awaitTermination()

  }

}
