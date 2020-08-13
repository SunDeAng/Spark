package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc:
 */
object Demo06_sample {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6,7,8,9), 3)

    //不放回，每个元素出现概率为0
    //val sampleRDD = rdd.sample(false, 0)

    //不放回，每个元素出现概率为1
    //val sampleRDD = rdd.sample(false, 1)

    //放回，每个元素出现概率为0.5
    val sampleRDD = rdd.sample(true, 0.5)

    sampleRDD.collect().foreach(println)

    //行动算子takeSample(是否放回，抽几个数)
    val res: Array[Int] = rdd.takeSample(false, 3)
    println(res)

    //关闭资源
    sc.stop()

  }

}
