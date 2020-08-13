package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-18
 * @Desc:
 */
object Demo04_Action {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ssc = new StreamingContext(conf, Seconds(3))

    //从指定端口读数据
    val lineDS = ssc.socketTextStream("hadoop102", 9999)

    lineDS.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFiles("output/atguigu")

    ssc.start()
    ssc.awaitTermination()

  }

}
