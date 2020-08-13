package com.atguigu.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-18
 * @Desc:
 */
object Demo01_Transform {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ssc = new StreamingContext(conf, Seconds(3))

    //从指定端口读数据
    val lineDS = ssc.socketTextStream("hadoop102", 9999)
    //DS->RDD
    val res = lineDS.transform(
      rdd => {
        val sortRDD = rdd.flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
          .sortBy(-_._2)
        sortRDD
      }
    )

    res.print

    ssc.start()
    ssc.awaitTermination()

  }

}
