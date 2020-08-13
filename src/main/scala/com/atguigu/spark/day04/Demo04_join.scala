package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08
 * @Desc:
 *
 */
object Demo04_join {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)


    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))


    //相当于内连接
    val newRDD: RDD[(Int, (String, Int))] = rdd.join(rdd1)
    //val new1RDD: RDD[(Int, (Int, String))] = rdd1.join(rdd)
    newRDD.collect().foreach(println)

    //左外连接
    val outJoinRDD: RDD[(Int, (String, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    outJoinRDD.collect().foreach(println)

    //右外连接
    val rightJoinRDD: RDD[(Int, (Option[String], Int))] = rdd.rightOuterJoin(rdd1)
    rightJoinRDD.collect().foreach(println)

    //全连接
    val fullRDD: RDD[(Int, (Option[String], Option[Int]))] = rdd.fullOuterJoin(rdd1)
    fullRDD.collect().foreach(println)

    //关闭资源
    sc.stop()

  }

}
