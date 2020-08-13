package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *      distinct 对RDD中的数据去重   有shuffle过程
 *
 *      底层代码
 *      转化为元祖(x,null)   按key聚合(key,null)                      取元祖中的key
 *      map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
 *
 *
 */
object Exer07_distinct {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd = sc.makeRDD(List(1, 2, 3, 4,3,1,7,2,9),3)
    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")

    //去重操作  默认产生的分区和原RDD分区个数相同
    val disRDD: RDD[Int] = rdd.distinct()
    disRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")

    //去重操作  可以指定分区数
    val dis1RDD: RDD[Int] = rdd.distinct(2)
    dis1RDD.mapPartitionsWithIndex{
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
