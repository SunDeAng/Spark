package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 * sample 随机抽样
 * 对RDD中的元素进行随机抽样
 * withReplacement: Boolean, 是否抽样放回
 * fraction: Double,
 * withReplacement=false，表示rdd中元素被抽取的概率  [0,1]
 * withReplacement= true，表示期望RDD中元素被抽取的次数  >0
 * seed: Long = Utils.random.nextLong    一般不需要设置
 *
 * sample():可以指定概率抽取，指定范围，是否放回
 * takeSample():等概率抽取，可以指定范围，是否放回，抽取几个
 *
 *
 */
object Exer06_sample {

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

    //1、false:抽取不放回   0:抽取概率为0  此算子会抽取对所有元素按照概率抽取
    val samRDD1: RDD[Int] = rdd.sample(false, 0)

    //2、false:抽取不放回   1:抽取概率为1  会取出全部数据
    val samRDD2: RDD[Int] = rdd.sample(false, 0)

    //3、true:抽取后放回   0.5:抽取概率为0.5  会对所有数据按照0.5的概率决定是否取出
    val samRDD3: RDD[Int] = rdd.sample(true, 0.5)
    //输出抽取结果
    samRDD3.collect().foreach(println)



    //takeSample():等概率抽取，可以指定范围，是否放回，抽取几个

    //不放回，抽取1个
    val takeRDD: Array[Int] = rdd.takeSample(false, 1)
    //输出抽取结果
    takeRDD.foreach(println)


    //关闭资源
    sc.stop()

  }

}
