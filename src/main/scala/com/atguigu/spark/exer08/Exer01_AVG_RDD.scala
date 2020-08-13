package com.atguigu.spark.exer08

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc: 使用RDD算子求平均年龄
 */
object Exer01_AVG_RDD {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //0.创建数据集RDD
    val rdd = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))

    //转换
    val res = rdd.map {
      case (name, age) => {
        (age, 1)
      }
    }.reduce(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    println(res._1 / res._2)

    //关闭资源
    sc.stop()

  }

}
