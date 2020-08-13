package com.atguigu.spark.exer03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *       flatMap  : 对RDD中的元素进行扁平化处理，要求RDD中的数据是可迭代的
 *
 */
object Exer01_flatMap {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd = sc.makeRDD(List(List(1, 2, 3, 4), List(5, 6, 7, 8), List(9)))

    //数据扁平化处理
    val result = rdd.flatMap(list => list)

    //输出
    result.collect().foreach(println)


    //关闭资源
    sc.stop()

  }

}
