package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc: 通过groupBy实现wordCount功能
 */
object Demo04_groupBy_WordCount {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    /*
    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello world", "hello scala", "spark scala"))


    普通方式实现
    //扁平映射
    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))

    wordRDD.collect().foreach(println)

    //对wordRDD进行结构转换
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    mapRDD.collect().foreach(println)

    //对mapRDD进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(t => t._1)
    groupRDD.collect().foreach(println)

    //对groupRDD进行结构转换
    val resRDD = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }

    resRDD.collect().foreach(println)
*/
/*

    //普通实现方式2
    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello world", "hello scala", "spark scala"))

    //扁平映射
    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
    wordRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word => word)
    groupRDD.collect().foreach(println)

    //对groupRDD结构转换
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }
    resRDD.collect().foreach(println)
*/

   /*
    //复杂实现1
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello world", 2), ("hello scala", 3), ("spark scala", 2)))

    //对rdd结构转换，将元祖转为字符串
    val newRDD = rdd.map {
      case (word, count) => {
        (word + " ") * count
      }
    }
    newRDD.collect().foreach(println)

    val flatMapRDD = newRDD.flatMap(_.split(" "))
    flatMapRDD.collect().foreach(println)

    val groupRDD = flatMapRDD.groupBy(word => word)
    groupRDD.collect().foreach(println)

    val resRDD = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }

    resRDD.collect().foreach(println)
*/

    //复杂实现2
    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello world", 2), ("hello scala", 3), ("spark scala", 2)))

    //对rdd结构转换，将元祖转为字符串
    val newRDD = rdd.flatMap {
      case (wordStr, count) => {
        wordStr.split(" ").map((_,count))
      }
    }
    newRDD.collect().foreach(println)

    println("--------------")

    //安装元祖第一个元素分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newRDD.groupBy(_._1)
    groupRDD.collect().foreach(println)
    println("--------------")

    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }
    resRDD.collect().foreach(println)

    //关闭资源
    sc.stop()

  }

}
