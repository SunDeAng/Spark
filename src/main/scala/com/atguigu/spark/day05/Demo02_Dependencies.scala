package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-11
 * @Desc:
 */
object Demo02_Dependencies {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)


    //准备数据
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    //查看血缘关系
    println(strRDD.dependencies)
    println("-----------")

    //flatmap
    val flatMapRDD = strRDD.flatMap(_.split(" "))
    //查看血缘关系
    println(flatMapRDD.dependencies)
    println("-----------")

    //map转为元祖
    val mapRDD = flatMapRDD.map((_, 1))
    //查看血缘关系
    println(mapRDD.dependencies)
    println("-----------")

    //reduceByKey
    val resRDD = mapRDD.reduceByKey(_ + _)
    //查看血缘关系
    println(resRDD.dependencies)
    println("-----------")

    resRDD.collect().foreach(println)


    //关闭资源
    sc.stop()

  }

}
