package com.atguigu.spark.exer04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-10
 * @Desc:
 */
object Exer02_sortByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    //默认按照升序排
    val sort1RDD = rdd.sortByKey()
    //降序排序
    val sort2RDD = rdd.sortByKey(false)
    //按照value进行排序
    val sort3RDD = rdd.sortBy(_._2)

    //关闭资源
    sc.stop()


  }

}
