package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-07 13:43
 * @Desc: 本案例为从集合创建RDD分区源码
 *
 */
object Demo03_PartitionScource_Memory {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //使用mkRDD()创建RDD  默认创建指定cpu核数个分区  第二个参数可以指定分区
    //val rdd = sc.makeRDD(Array(1, 2, 3, 4))

    //1）4个数据，设置4个分区，输出：0分区->1，1分区->2，2分区->3，3分区->4
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)

    //2）4个数据，设置3个分区，输出：0分区->1，1分区->2，2分区->3,4
    //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 3)

    //3）5个数据，设置3个分区，输出：0分区->1，1分区->2、3，2分区->4、5
    //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5), 3)



    //输出
    rdd.saveAsTextFile("E:\\Project\\Spark\\output")

    //关闭资源
    sc.stop()

  }

}
