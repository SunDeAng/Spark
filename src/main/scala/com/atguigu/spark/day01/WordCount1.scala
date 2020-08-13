package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-06 18:16
 * @Desc:
 */
object WordCount1 {

  def main(args: Array[String]): Unit = {

    //0、设置执行项目名，并且设置运行模式
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    //1、新建SC，将配置信息加载到SC中
    val sc = new SparkContext(conf)

    //2、加载数据
      //hello scala
      //hello world
      //world scala
      //spark scala
    val initFile = sc.textFile("E:\\Project\\Spark\\src\\input\\1.txt")

    //3、数据扁平化切分
      //hello scala hello world world scala spark scala
    val rddFile: RDD[String] = initFile.flatMap(_.split(" "))

    //4、数据映射
    //map:((hello,1),(scala,1),(hello,1),(world,1),(world,1),(scala,1),(spark,1),(scala,1))
    val mapFile = rddFile.map((_, 1))

    //5、数据统计
      //map:((hello,2),(scala,3),(world,2),(spark,1))
    val resultFile: RDD[(String, Int)] = mapFile.reduceByKey(_ + _)

    //6、行为算子(方法)提交执行
    val res: Array[(String, Int)] = resultFile.collect()

    //7、输出数据
    res.foreach(println)

    //8、关闭资源
    sc.stop()


  }

}
