package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *        doubleValue 双value类型
 *        两个rdd之间的运算
 *
 *        合集  union
 *        差集  scala:diff ---> spark:subtract
 *        交集  scala:intersect --->  spark:intersection
 *        拉练  zip
 *          -拉链的要求
 *              要求每个分区中的元素个数相同  (scala中无此要求)
 *              要求分区数相同
 *
 */
object Exer10_doubleValue {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7),2)

    //1、合集  union
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    unionRDD.collect().foreach(println)

    println("------------------")

    //2、差集  diff  --> subtract
    val sunRDD = rdd1.subtract(rdd2)
    sunRDD.collect().foreach(println)

    println("------------------")

    //3、交集  intersect  --> intersection
    val interRDD = rdd1.intersection(rdd2)
    interRDD.collect().foreach(println)

    println("------------------")

    //4、拉链  zip
    val zipRDD = rdd1.zip(rdd2)
    zipRDD.collect().foreach(println)

    println("------------------")


    //关闭资源
    sc.stop()

  }

}
