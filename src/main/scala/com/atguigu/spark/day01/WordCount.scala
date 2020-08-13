package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-06 18:16
 * @Desc: 此为精简版WC，且执行于集群中的版本，详细本地版本见WordCount1
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    //0、设置执行项目名，并且设置运行模式
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("yarn")

    //1、新建SC，将配置信息加载到SC中
    val sc = new SparkContext(conf)

    //2、加载数据
      //hello scala
      //hello world
      //world scala
      //spark scala
    val initFile = sc.textFile(args(0))

    //3、处理数据
    initFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))

    //4、测试数据
    //res.foreach(println)

    //8、关闭资源
    sc.stop()


  }

}
