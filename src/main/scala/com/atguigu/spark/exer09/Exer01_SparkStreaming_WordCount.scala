package com.atguigu.spark.exer09

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-17
 * @Desc:
 */
object Exer01_SparkStreaming_WordCount {

  def main(args: Array[String]): Unit = {

    //创建配置信息
    val conf = new SparkConf().setAppName("SparkSteaming").setMaster("local[*]")
    //常见SparkStreaming的入口StreamingContext
    //第二个参数为时间间隔
    val ssc = new StreamingContext(conf, Seconds(3))

    //读取指定端口获取数据，数据是以流的形式，源源不断传输过来
    val lineDS = ssc.socketTextStream("hadoop102", 9999)

    //对传输过来的数据进行扁平化映射
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))
    //对数据进行结构转换
    val mapDS = flatMapDS.map((_, 1))
    //对数据进行整合
    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)

    //用行动算子执行以上操作
    resDS.print

    //以上操作的简化
    //lineDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print


    //启动SparkSteaming
    ssc.start()
    //设置等待关闭，可以一直接受数据
    ssc.awaitTermination()

  }

}
