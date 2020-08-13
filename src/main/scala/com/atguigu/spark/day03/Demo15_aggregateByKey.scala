package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08
 * @Desc:
 *       aggregateByKey 根据key对分区内以及分区间进行聚合操作
 *
 *
 */
object Demo15_aggregateByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd= sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("---------------------")

    //需求：分区最大值求和
//    rdd.aggregateByKey(0)((a,b)=>math.max(a,b),(x,y)=>(x+y))
val resRDD = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    resRDD.collect().foreach(println)

    //关闭资源
    sc.stop()

  }

}
