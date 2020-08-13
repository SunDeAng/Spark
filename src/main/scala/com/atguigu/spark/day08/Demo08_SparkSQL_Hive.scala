package com.atguigu.spark.day08

import org.apache.spark.sql.SparkSession

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Demo08_SparkSQL_Hive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark_Hive")
      .enableHiveSupport()
      .getOrCreate()

    //spark.sql("show tables").show()
    spark.sql(
      """
        |SELECT
        |*,
        |RANK() OVER(PARTITION by t2.area ORDER by t2.click_count DESC ) rank
        |FROM
        |(SELECT t1.area,COUNT(*) click_count,t1.product_name
        |FROM
        |(SELECT pi.product_name ,uvc.click_product_id ,ci.city_name ,ci.area
        |FROM
        |`default`.user_visit_action uvc
        |join
        |`default`.city_info ci
        |on
        |uvc.city_id = ci.city_id
        |join
        |`default`.product_info pi
        |on uvc.click_product_id = pi.product_id ) t1
        |group by t1.product_name,t1.area)  t2
        |""".stripMargin).show()


  }

}
