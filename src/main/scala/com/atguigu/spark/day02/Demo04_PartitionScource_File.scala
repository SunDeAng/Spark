package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-07 13:43
 * @Desc: 本案例为从文件创建RDD分区源码
 *
 */
object Demo04_PartitionScource_File {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //读取本地文件系统
    //1）默认分区的数量：默认取值为当前核数和2的最小值
    //val rdd: RDD[String] = sc.textFile("E:\\Project\\Spark\\input1")

    //2）输入数据1-4，每行一个数字；输出：0=>{1、2} 1=>{3} 2=>{4} 3=>{空}
    val rdd: RDD[String] = sc.textFile("E:\\Project\\Spark\\input1\\3.txt",3)

    //3）输入数据1-4，一共一行；输出：0=>{1234} 1=>{空} 2=>{空} 3=>{空}
    //val rdd: RDD[String] = sc.textFile("E:\\Project\\Spark\\input1\\4.txt",3)

    //输出
    rdd.saveAsTextFile("E:\\Project\\Spark\\output")
    //rdd.collect().foreach(println)

    //关闭资源
    sc.stop()

  }

}
