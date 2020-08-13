package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08
 * @Desc:
 *
 */
object Demo01_combineByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)


    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //求出每个key的平均数

    //方案一:groupByKey
    //按照key分组
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    //对groupRDD结构转换
    val resRDD = groupRDD.map {
      case (name, datas) => {
        (name, datas.sum / datas.size)
      }
    }
    resRDD.collect().foreach(println)


    //方案二：reduceByKey

    //对个数进行计数,转换结构
    val mapRDD = rdd.map {
      case (name, score) => {
        (name, (score, 1))
      }
    }
    mapRDD.collect().foreach(println)

    //聚合操作
    val reduceRDD = mapRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    })
    reduceRDD.collect().foreach(println)

    //通过map进行结构转换求平均成绩
    val resultRDD = reduceRDD.map {
      case (name, (scoreSum, countSum)) => {
        (name, scoreSum / countSum)
      }
    }
    resultRDD.collect().foreach(println)


    //方案三：combineByKey
   /*
   val comRDD = rdd.combineByKey(
      num => (num, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (tup1: (Int, Int), tup2: (Int, Int)) => {
        (tup1._1 + tup2._1, tup1._2, tup2._2)
      }
    )

    val result1RDD = comRDD.map {
      case (name, (scoreSum, countSum)) => {
        (name, scoreSum / countSum)
      }
    }
    result1RDD.collect().foreach(println)
*/

    //关闭资源
    sc.stop()

  }

}
