package com.atguigu.spark.day05

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-11
 * @Desc:
 */
object DemoXX_Mysql_read {

  def main(args: Array[String]): Unit = {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "123456"


    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)
/*

    new JdbcRDD(
      sc,
      ()={
        Class.forName(driver)
        Driver
      }
    )
*/


    //关闭资源
    sc.stop()

  }

}
