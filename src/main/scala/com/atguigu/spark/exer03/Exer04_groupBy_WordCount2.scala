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
object Exer04_groupBy_WordCount2 {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //普通实现方式2

    //1、准备数据
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    strRDD.collect().foreach(println)

    //2、进行flatMap()扁平化处理   (helllo Scala ......)
    val flatMapRDD = strRDD.flatMap(_.split(" "))

    //3、进行groupBy分组
    val groupRDD = flatMapRDD.groupBy(word=>word)

    //4、进行统计
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
