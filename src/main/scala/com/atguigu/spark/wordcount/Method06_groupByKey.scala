package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *        groupByKey 对RDD中相同key的元素进行分组
 *
 */
object Method06_groupByKey {
  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))

    //flatMap
    val flatMapRDD = strRDD.flatMap(_.split(" "))

    //2、分组求和
    val mapRDD = flatMapRDD.map((_, 1))

    //3、group
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

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
