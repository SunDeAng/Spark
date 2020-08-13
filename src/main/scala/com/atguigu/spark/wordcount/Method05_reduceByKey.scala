package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *        reduceByKey 对RDD中相同key的元素进行聚集
 *
 */
object Method05_reduceByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))

    //flatmap
    val flatMapRDD = strRDD.flatMap(_.split(" "))

    //map转为元祖
    val mapRDD = flatMapRDD.map((_, 1))

    //reduceByKey
    val resRDD = mapRDD.reduceByKey(_ + _)

    resRDD.collect().foreach(println)


    //效果与上等价
    //strRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()


    //关闭资源
    sc.stop()

  }

}
