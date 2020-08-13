package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *        aggragateByKey  根据key对分区内和分区间的数据进行聚合
 *
 *                          默认值   分区内聚合规则       分区间聚合规则
 *        rdd.aggregateByKey(0)((a,b)=>math.max(a,b),(x,y)=>(x+y))
 *
 */
object Exer14_aggregateByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("------------------")

    //分区最大值求和
    //rdd.aggregateByKey(0)((a,b)=>math.max(a,b),(x,y)=>(x+y))
    val resRDD = rdd.aggregateByKey(0)(math.max(_, _), _ + _)

    resRDD.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("------------------")


    //关闭资源
    sc.stop()

  }

}
