package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08
 * @Desc:
 *
 */
object Demo14_groupByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    //按照key分组
    val groupRDD = rdd.groupByKey()

    groupRDD.collect().foreach(println)


    //分组后对value进行求和操作
    val sumRDD = groupRDD.map {
      case (key, datas) => {
        (key, datas.sum)
      }
    }
    sumRDD.collect().foreach(println)


    //关闭资源
    sc.stop()

  }

}
