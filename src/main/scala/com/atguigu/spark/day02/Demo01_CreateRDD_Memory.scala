package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-07 13:43
 * @Desc: 创建RDD的三种方式
 *       1、从集合中创建RDD
 *       2、从外部存储创建RDD
 *       3、从其他RDD创建
 *
 *       本案例为从集合创建RDD
 *       创建方法：
 *        -parallelize()
 *        -makeRDD()  本方法底层调用的是parallelize()
 *        -两个方法中均可传集合以及分区数
 */
object Demo01_CreateRDD_Memory {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //使用parallelize()创建rdd
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8))
    rdd.foreach(println)

    //获取分区数
    println("分区数：" + rdd.partitions.size)


    //使用mkRDD()创建RDD
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))
    rdd1.foreach(println)

    //获取分区数
    println("分区数：" + rdd1.partitions.size)

    //关闭资源
    sc.stop()

  }

}
