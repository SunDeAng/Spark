package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *      改变分区
 *        -coalesce
 *          默认不shuffle  有参数调节
 *          一般不用于缩减分区
 *        -rePartition
 *          底层调用coalesce  默认开启shuffle
 *          一般用于扩大分区
 *
 *
 */
object Exer08_changePartition {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd = sc.makeRDD(List(1, 2, 3, 4,5,6,7,8,9),3)
    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")

    //coalesce  缩减分区  默认无shuffle
    val caolRDD1 = rdd.coalesce(2)
    caolRDD1.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")

    //coalesce  扩大分区  默认无shuffle  不会重新排列数据  所有扩大分区无意义 输出与原同
    val caolRDD2 = rdd.coalesce(4)
    caolRDD2.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")

    //coalesce  扩大分区  需要  指定参数执行shuffle进行数据重排列
    val caolRDD3 = rdd.coalesce(4,true)
    caolRDD3.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")


    //rePartition 实际在扩大分区时，使用此算子  效果和使用coalesce启动shuffle相同
    val reRDD = rdd.repartition(4)
    reRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")

    //rePartition 缩减分区，底层使用的是coalesce并启动shuffle
    val reRDD1 = rdd.repartition(2)
    reRDD1.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")

    //关闭资源
    sc.stop()

  }

}
