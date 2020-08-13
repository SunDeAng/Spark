package com.atguigu.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 *       UDF    输入一行，返回一行
 *       UDAF   输入多行，返回一行
 *       UDTF   输入一行，返回多行   SparkSQL中无UDTF
 */
object Demo01_Avg_RDD {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._


    //需求：求平均年龄

    //方式一：使用sc算子实现
    val initRDD = spark.sparkContext.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))
    val res = initRDD.map {
      case (name, age) => {
        (age, 1)
      }
    }.reduce {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }
    println(res._1 / res._2)


    //关闭session
    spark.close()

  }

}
