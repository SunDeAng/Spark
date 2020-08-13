package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:24
 * @Desc: flatMap对RDD中元素进行扁平化处理
 *       要求RDD中的元素是可迭代的
 */
object Demo01_FlatMap {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3), List(4, 5), List(6, 7), List(8)))
    val newRDD = rdd.flatMap(list => list)
    newRDD.collect().foreach(println)



    //关闭资源
    sc.stop()

  }

}
