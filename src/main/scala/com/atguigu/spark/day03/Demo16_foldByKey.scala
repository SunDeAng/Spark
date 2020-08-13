package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08
 * @Desc:
 *
 */
object Demo16_foldByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a",1),("a",1),("a",1),("b",1),("b",1),("b",1),("b",1),("a",1))
    val rdd = sc.makeRDD(list,2)

    //wordCount   下两个同
    val resRDD = rdd.foldByKey(0)(_ + _)
    val res1RDD = rdd.aggregateByKey(0)(_ + _,_ + _)

    resRDD.collect().foreach(println)



    //关闭资源
    sc.stop()

  }

}
