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
 *       本案例为从外部存储系统创建RDD
 *        -外部存储系统：本地文件系统、Hadoop支持的数据集
 *        -创建方法：
 *          -本地获取文件方法:  sc.textFile("本地文件/文件夹地址")
 *          -集群获取文件方法:  sc.textFile("hdfs://hadoop102:8020/集群文件地址")
 */
object Demo02_CreateRDD_File {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //读取本地文件系统
    val rdd: RDD[String] = sc.textFile("E:\\Project\\Spark\\input")
    rdd.foreach(println)

    //读取集群文件系统  路径为：hdfs://hadoop102:8020/input
    val rdd1: RDD[String] = sc.textFile("hdfs://hadoop102:8020/input")
    rdd1.foreach(println)

    //关闭资源
    sc.stop()

  }

}
