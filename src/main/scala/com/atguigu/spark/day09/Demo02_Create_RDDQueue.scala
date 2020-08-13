package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @Author: Sdaer
 * @Date: 2020-07-17
 * @Desc: 通过rdd队列创建DStream
 */
object Demo02_Create_RDDQueue {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ssc = new StreamingContext(conf, Seconds(3))

    //创建RDD队列
    var queue:mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    //创建离散化流  第二个参数：oneAtTime 一个采集周期是否处理一个RDD，默认为true
    val queueDS = ssc.queueStream(queue,false)

    //对DS操作
    queueDS.map((_,1)).reduceByKey(_+_).print

    //开启采集线程进行数据采集
    ssc.start()
    //向队列中放数据
    for (i <- 1 to 5){
      //通过SSC获取SC创建RDD并放在队列中
      queue.enqueue( ssc.sparkContext.makeRDD(1 to 5))
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
