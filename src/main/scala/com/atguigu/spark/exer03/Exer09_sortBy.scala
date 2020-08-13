package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *        sortBy  对RDD中的元素排序
 *
 *
 */
object Exer09_sortBy {

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

    //1、groupBy：默认升序排序
    val ascRDD: RDD[Int] = rdd.sortBy(elem => elem)
    ascRDD.collect().foreach(println)

    println("------------------")

    //2、groupBy：降序1
    val descRDD: RDD[Int] = rdd.sortBy(elem => -elem)
    descRDD.collect().foreach(println)

    println("------------------")

    //3、groupBy：降序2   通过第二个参数指定false执行降序
    val descRDD1: RDD[Int] = rdd.sortBy(elem=>elem,false)
    descRDD1.collect().foreach(println)

    println("------------------")

    //4、groupBy：字符串排序   按照字典(ANSCI)排序,先按照第一个字母排，再按照第二个字母排
    val rdd1: RDD[String] = sc.makeRDD(List("1","11","2","6","3"),3)
    val strRDD = rdd1.sortBy(elem => elem)
    strRDD.collect().foreach(println)

    println("------------------")

    //5、groupBy：元祖排序   先按照第一个字母排，再按照第二个字母排
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((2, 1), (1, 2), (1, 1), (2, 2)))
    val tupRDD = rdd2.sortBy(elem => elem)
    tupRDD.collect().foreach(println)

    println("------------------")

    //关闭资源
    sc.stop()

  }

}
