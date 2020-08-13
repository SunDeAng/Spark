package com.atguigu.spark.exer04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-10
 * @Desc:
 */
object Exer01_combineByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //求出每个学生的平均成绩

    //方案一   groupByKey
    val groupRDD = rdd.groupByKey()
    val res1RDD = groupRDD.map {
      case (name, datas) => {
        (name, datas.sum / datas.size)
      }
    }
    res1RDD.collect().foreach(println)


    //方案二 reduceByKey
    val map2RDD = rdd.map {
      case (name, datas) => {
        (name, (datas, 1))
      }
    }

    val reduceRDD = map2RDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    })

    val res2RDD = reduceRDD.map {
      case (name, (scoreSum, scoreCount)) => {
        (name, scoreSum / scoreCount)
      }
    }
    res2RDD.collect().foreach(println)


    //方案3 使用combineByKey
    val comRDD = rdd.combineByKey(
      num => (num, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (tup1: (Int, Int), tup2: (Int, Int)) => {
        (tup1._1 + tup2._1, tup1._2 + tup2._2)
      }
    )


    val res3RDD = comRDD.map {
      case (name, (scoreSum, countSum)) => {
        (name, scoreSum / countSum)
      }
    }
    res3RDD.collect().foreach(println)


    //关闭资源
    sc.stop()


  }

}
