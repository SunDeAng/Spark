package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-18
 * @Desc:
 */
object Demo03_window {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点目录，用于保存状态
    ssc.checkpoint("chk")

    //从指定端口读数据
    val lineDS = ssc.socketTextStream("hadoop102", 9999)

/*

    //window
    //设置窗口  窗口大小6s，滑动步长3s 均要求为采集周期的整数倍
    val windowDS = lineDS.window(Seconds(6), Seconds(3))
    val flatMapDS = windowDS.flatMap(_.split(" "))
    val mapDS = flatMapDS.map((_, 1))
    val resDS = mapDS.reduceByKey(_ + _)

    resDS.print()
*/
    //reduceByWindow
    lineDS.flatMap(_.split(" ")).map(_.toInt).reduceByWindow(_+_,Seconds(6),Seconds(3)).print()


    ssc.start()
    ssc.awaitTermination()

  }

}
