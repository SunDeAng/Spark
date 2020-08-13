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
object Method01_groupBy_1 {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //普通实现方式1

    //1、准备数据
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    strRDD.collect().foreach(println)

    //2、进行flatMap()扁平化处理   (helllo Scala ......)
    val flatMapRDD = strRDD.flatMap(_.split(" "))

    //3、进行map()结构转换为元祖    ((hello,1),(scala,1),......)
    val mapRDD = flatMapRDD.map(data => (data, 1)) //  flatMapRDD.map((_,1)))

    //4、进行groupBy分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(t => t._1)

    //5、进行统计
    val result: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }

    //6、输出
    result.collect().foreach(println)




    //关闭资源
    sc.stop()

  }

}
