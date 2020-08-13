package com.atguigu.spark.exer03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *        groupByKey 对RDD中相同key的元素进行分组
 *
 */
object Exer13_groupByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    //1、按照key分组
    val groupRDD = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    println("------------------")

    //2、分组求和
    val resRDD = groupRDD.map {
      case (key, values) => {
        (key, values.sum)
      }
    }
    resRDD.collect().foreach(println)


    //关闭资源
    sc.stop()

  }

}
