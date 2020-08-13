package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc:
 *       sortBy 排序
 */
object Demo10_sortBy {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(6, 8, 4, 3, 5, 1,7,2,9), 3)

    //默认是升序
    val newRDD: RDD[Int] = rdd.sortBy(elem => elem)
    newRDD.collect().foreach(println)

    //加false表示降序
    //val descRDD: RDD[Int] = rdd.sortBy(elem => -elem)
    val descRDD: RDD[Int] = rdd.sortBy(elem => elem,false)
    descRDD.collect().foreach(println)
    println("=================")

    //字符串
    val rdd1: RDD[String] = sc.makeRDD(List("21", "11", "4", "3", "5", "1","7","2","9"), 3)

    val newrdd1 = rdd1.sortBy(elem => elem)
    newrdd1.collect().foreach(println)

    //元祖  先按照第一个元素排序，再按照第二个元素排序
    val rddy = sc.makeRDD(List((6, 8), (4, 3), (5, 1),(7,2),(1,9)), 3)
    val newRDDY = rddy.sortBy(elem => elem)
    newRDDY.collect().foreach(println)

    //关闭资源
    sc.stop()

  }

}
