package com.atguigu.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08
 * @Desc:
 *       groupBy  对RDD中的元素按照指定的规则进行分组
 *       通过groupBy进行WordCount
 *
 *
 */
object Method03_groupBy_3 {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //复杂实现方式1

    //1、准备数据
    val strRDD: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))
    strRDD.collect().foreach(println)

    //2、对数据进行Map结构转换  将元祖转换成字符串
    val newRDD = strRDD.map {
      case (data, count) => {
        (data + " ") * count
      }
    }

    //3、进行flatMap()扁平化处理   (helllo Scala ......)
    val flatMapRDD = newRDD.flatMap(_.split(" "))

    //4、进行groupBy分组
    val groupRDD = flatMapRDD.groupBy(word=>word)

    //5、进行统计
    val result: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }

    //5、输出
    result.collect().foreach(println)




    //关闭资源
    sc.stop()

  }

}
