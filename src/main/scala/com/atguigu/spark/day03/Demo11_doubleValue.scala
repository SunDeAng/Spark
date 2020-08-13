package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc:
 *       双Value
 */
object Demo11_doubleValue {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7,8), 2)

    //合集  union
    val newRDD = rdd1.union(rdd2)
    newRDD.collect().foreach(println)

    println("-------------------")

    //差集  diff--->substract
    val subRDD = rdd1.subtract(rdd2)
    subRDD.collect().foreach(println)

    println("-------------------")

    //交集  intersect--->intersection
    val intRDD = rdd1.intersection(rdd2)
    intRDD.collect().foreach(println)

    println("-------------------")

    //拉练  zip   spark中rdd1和rdd2个数不匹配时，会报错
    //要求每个分区中元素个数相同
    //要求分区个数也要相同
    val zipRDD = rdd1.zip(rdd2)
    zipRDD.collect().foreach(println)

    println("-------------------")


    //关闭资源
    sc.stop()

  }

}
