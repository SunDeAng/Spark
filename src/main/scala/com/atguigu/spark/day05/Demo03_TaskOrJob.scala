package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-11
 * @Desc: spark的Job调度
 *       重要的概念：
 *          -集群
 *            Standalone  Yarn
 *          -应用
 *            编写的Spark的Application，一般创建一个SparkCOntext
 *            表示创建了一个应用
 *
 *            一个集群中可以有多个应用
 *
 *          -Job
 *            每次触发行动操作，都会提交一个Job
 *            一个Spark应用中有多个Job
 *          -Stage
 *            阶段，根据当前Job中宽依赖的数量来划分阶段
 *            阶段的数量 = 宽依赖的数量+1
 *
 *          -Task
 *            任务，每个阶段由多个Task组成
 *            每个阶段最后一个RDD中的分区数就是当前阶段的任务数
 */
object Demo03_TaskOrJob {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)


    //准备数据
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    //查看血缘关系
    println(strRDD.dependencies)
    println("-----------")

    //flatmap
    val flatMapRDD = strRDD.flatMap(_.split(" "))
    //查看血缘关系
    println(flatMapRDD.dependencies)
    println("-----------")

    //map转为元祖
    val mapRDD = flatMapRDD.map((_, 1))
    //查看血缘关系
    println(mapRDD.dependencies)
    println("-----------")

    //reduceByKey
    val resRDD = mapRDD.reduceByKey(_ + _)
    //查看血缘关系
    println(resRDD.dependencies)
    println("-----------")

    resRDD.collect().foreach(println)


    //关闭资源
    sc.stop()

  }

}
