package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-18
 * @Desc:
 */
object Demo02_updateStateByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点目录，用于保存状态
    ssc.checkpoint("chk")

    //从指定端口读数据
    val lineDS = ssc.socketTextStream("hadoop102", 9999)
    //DS->RDD

    val mapDS = lineDS.flatMap(_.split(" ")).map((_, 1))
    //只会对当前采集周期聚合
    val reducrDS = mapDS.reduceByKey(_ + _)

    //记录上个采集周期的状态，和当前采集周期数据进行聚合
    val resDS = mapDS.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(state.getOrElse(0) + seq.sum)
      }
    )
    resDS.print



    ssc.start()
    ssc.awaitTermination()

  }

}
