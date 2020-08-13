package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc: 使用指定分区器按照key对rdd重新分区
 *
 */
object Demo12_partitionBy {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)

    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("---------------------")


    //单值无此算子，kv有此算子，此算子是隐式转换拓展而来的
    val newRDD = rdd.partitionBy(new HashPartitioner(2))
    newRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("---------------------")

    val myRDD = rdd.partitionBy(new MyPartitioner(2))
    myRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("---------------------")

    //关闭资源
    sc.stop()

  }

}

//自定义分区器
class MyPartitioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {

    1

  }
}
