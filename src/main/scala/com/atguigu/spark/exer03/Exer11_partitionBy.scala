package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *        partitionBy   按照某种规则分区
 *
 */
object Exer11_partitionBy {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1,"aaa"),(2,"bbb"),(3,"cccc")),3)
    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("------------------")


    //按照hash分区
    val hashRDD: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    hashRDD.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("------------------")

    //按照自定义分区
    val myRDD = rdd.partitionBy(new MyPartitioner(2))
    myRDD.mapPartitionsWithIndex(
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

//自定义分区规则
class MyPartitioner(partitions: Int) extends Partitioner{

  //获取分区的个数
  override def numPartitions: Int = partitions

  //执行分区规则
  override def getPartition(key: Any): Int = {

    1 //不管key是什么，全部放在1号分区

  }
}
