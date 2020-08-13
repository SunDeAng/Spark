package com.atguigu.spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Sdaer
 * @Date: 2020-07-14
 * @Desc:
 */
object Demo_SparkSession {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._

    //读取json文件创建DF
    val df: DataFrame = spark.read.json("input/test.json")
    //df.show()

    //----------通过SQL风格语法操作DF
    //创建临时视图
    df.createOrReplaceTempView("people")
    //编写SQL对临时视图进行查询
    val resSQL = spark.sql("select * from people")
    //resSQL.show()

    //---------通过DSL风格语法操作DF
    val resSQL1 = df.select("*")
    //resSQL1.show()


    //rdd=>df=>ds
    val rdd = spark.sparkContext.makeRDD(List(("zhangsan", 20), ("lisi", 30)))

    //rdd=>df
    // 如果不指定列名，没办法映射为DS
    // 如果指定的列名和样例类属性名不一致，没办法映射为DS。
    // 映射样例类的时候，是按照列的名字映射的
    val rddToDF = rdd.toDF("name", "age")

    //rdd=>ds
    val rddToDS = rdd.toDS()

    //df=>ds
    val dfToDs = rddToDF.as[User01]



    //ds=>df=>rdd

    //ds=>rdd
    val dsToRDD = dfToDs.rdd
    //df=>rdd
    val dfToRDD = dfToDs.rdd
    //ds=>df
    val dsToDf = rddToDS.toDF()
    dsToDf.show()

    //关闭session
    spark.close()

  }

}

//样例类
case class User01(name:String,age:Int)
