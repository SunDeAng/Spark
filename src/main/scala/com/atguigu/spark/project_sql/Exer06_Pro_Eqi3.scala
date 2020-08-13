package com.atguigu.spark.project_sql

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc:
 */
object Exer06_Pro_Eqi3 {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //	读取原始数据
    val lineRDD = sc.textFile("input3")
    //	将原始数据映射为样例类
    val userVisitAction = lineRDD.map {
      line => {
        val field = line.split("_")
        UserVisitAction(
          field(0),
          field(1).toLong,
          field(2),
          field(3) toLong,
          field(4),
          field(5),
          field(6) toLong,
          field(7) toLong,
          field(8),
          field(9),
          field(10),
          field(11),
          field(12) toLong
        )
      }
    }
    //	将原始数据根据session进行分组
    val sessionRDD = userVisitAction.groupBy(_.session_id)
    //	将分组后的数据根据时间进行排序（升序）
    val dateRDD = sessionRDD.mapValues(
        datas => {
          datas.toList.sortWith {
            case (left, right) => {
              left.action_time < right.action_time
            }
          }
        }
    )
    //	将排序后的数据进行结构的转换(pageId,1)
    val map1RDD = dateRDD.map {
      dates => {
        val array = dates._2.toArray
        (array(0).page_id,1)
      }
    }

    //	计算分母-将相同的页面id进行聚合统计(pageId,sum)
    val sumPagId = map1RDD.reduceByKey(_ + _)
    sumPagId.take(300).foreach(println)
    //	计算分子-将页面id进行拉链，形成连续的拉链效果，转换结构(pageId-pageId2,1)

    //	将转换结构后的数据进行聚合统计(pageId-pageId2,sum)
    //	计算页面单跳转换率

    //关闭资源
    sc.stop()

  }

}
