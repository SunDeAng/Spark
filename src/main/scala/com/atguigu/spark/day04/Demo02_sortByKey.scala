package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08
 * @Desc:
 *
 */
object Demo02_sortByKey {

  /*implicit object MyOrdering extends Ordering[Student]{
    override def compare(x: Student, y: Student): Int = {
      var res = x.name.compareTo(y.name)
      if (res ==0 ){
        res = y.age - x.age
      }
      res
    }
  }*/


  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)


    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    //按照key升序排序
    val newRDD = rdd.sortByKey()
    newRDD.collect().foreach(println)

    //按照key降序排序
    val new1RDD = rdd.sortByKey(false)
    new1RDD.collect().foreach(println)

    //value升序排序
    val new2RDD = rdd.sortBy(_._2)
    new2RDD.collect().foreach(println)


    val str1: Student = new Student("aa", 10)
    val str2: Student = new Student("bb", 30)
    val str3: Student = new Student("cc", 40)
    val str4: Student = new Student("dd", 20)
    val str5: Student = new Student("aa", 20)

    val stdRDD = sc.makeRDD(List(str1, str2, str3, str4, str5))


    //会产生序列化错误，如果没继承Serializable
    val nameRDD = stdRDD.sortBy(std => std.name)
    nameRDD.collect().foreach(println)


    //关闭资源
    sc.stop()

  }

}

class Student(var name:String,var age:Int) extends Serializable with Ordered[Student]{

  override def compare(that: Student): Int = {
    var res = this.name.compareTo(that.name)
    if (res ==0 ){
      res = this.age - that.age
    }
    res
  }
}
