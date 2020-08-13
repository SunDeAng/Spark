package com.atguigu.spark.project_core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc:
 */
object Project02_Eqi3 {

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

    //计算分母
    //1、转换结构  =>    (page_id,1)
    val mapFMRDD = userVisitAction.map {
      case uva => {
        (uva.page_id, 1)
      }
    }
    //2、聚合
    val fmMap = mapFMRDD.reduceByKey(_ + _).collectAsMap()


    //计算分子
    //1、按照session_id 分组
    val groupRDD = userVisitAction.groupBy(_.session_id)
    //2、对分组数据进行按照时间排序
    val mapValueRDD = groupRDD.mapValues(
      datas => {
        //排序
        val sortRDD = datas.toList.sortBy(_.action_time)
        //转换，只保留page_id
        val userVisitList = sortRDD.map(_.page_id)
        //拉链
        val pageFlowList = userVisitList.zip(userVisitList.tail)
        //转换
        pageFlowList.map {
          case (pagA, pagB) => {
            (pagA + "_" + pagB, 1)
          }
        }
      }
    )

    //3、转换结构
    val pageFlowRDD = mapValueRDD.map(_._2).flatMap(list => list)

    //4聚合
    val fzRDD = pageFlowRDD.reduceByKey(_+_)
    //fzRDD.take(100).foreach(println)

    //5、计算跳转
    fzRDD.foreach{
      case (pageATopagB,fz)=>{
        val pagAID = pageATopagB.split("_")(0)
        val fm = fmMap.getOrElse(pagAID.toLong, 1)
        println(pageATopagB+"---->"+fz.toDouble/fm)
      }
    }

    //关闭资源
    sc.stop()

  }

}
