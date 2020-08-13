package com.atguigu.spark.exer09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @Author: Sdaer
 * @Date: 2020-07-17
 * @Desc:
 */
object Exer02_Create_RDDQueue {

  def main(args: Array[String]): Unit = {

    //创建配置信息
    val conf = new SparkConf().setAppName("SparkSteaming").setMaster("local[*]")
    //常见SparkStreaming的入口StreamingContext
    //第二个参数为时间间隔
    val ssc = new StreamingContext(conf, Seconds(3))

    //创建RDD队列
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    //创建离散化流  第二个参数：oneAtTime 一个采集周期是否只处理一个RDD，默认为True
    val queueDS: InputDStream[Int] = ssc.queueStream(queue,false)
    //对数据进行处理
    val res: DStream[(Int, Int)] = queueDS.map((_, 1)).reduceByKey(_ + _)
    //数据输出
    res.print

    //开启采集线程
    ssc.start()

    //向队列中放入数据
    for (i <- 1 to 5){
      queue.enqueue(ssc.sparkContext.makeRDD(1 to 5))
      Thread.sleep(2000)
    }

    //开始执行
    ssc.awaitTermination()

  }

}
