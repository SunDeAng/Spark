package com.atguigu.spark.exer08

import org.apache.spark.sql.SparkSession

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Exer08_Hive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Hive-Spark")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show tables").show()

  }

}
