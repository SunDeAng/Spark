package com.atguigu.spark.exer03

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
object Exer04_groupBy_WordCount4 {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //复杂实现方式2

    //1、准备数据
    val strRDD: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))
    strRDD.collect().foreach(println)

    //2、对数据进行flatMap结构转换  对RDD中元素进行扁平映射  ("Hello Scala", 2)==>(Hello,2),(Scala,2)
    val newRDD = strRDD.flatMap {
      case (data, count) => {
        data.split(" ").map((_,count))
      }
    }

    //4、进行groupBy分组
    val groupRDD = newRDD.groupBy(_._1)

    //5、进行统计
    val result: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }

    //5、输出
    result.collect().foreach(println)




    //关闭资源
    sc.stop()

  }

}
