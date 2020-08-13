package com.atguigu.spark.project_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc:
 */
object Project02_Eqi3_2 {

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

    //---------------需求三实现--------------
    //1.计算分母
    //1.1 定义一个map集合 存放页面的总访问次数
    val fmMap: collection.Map[Long, Int] = userVisitAction.map(userAction=>(userAction.page_id,1)).reduceByKey(_+_).collectAsMap()

    //2.计算分子
    //2.1按照sessionId对用户访问行为RDD数据进行分组
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitAction.groupBy(_.session_id)

    val mapValueRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => {
        //2.2对分组之后的数据进行排序
        val sortList: List[UserVisitAction] = datas.toList.sortBy(_.action_time)
        //2.3对排序后的数据结构进行转换，只保留页面的id
        val userVisitList: List[Long] = sortList.map(_.page_id)
        //2.4通过拉链 形成页面单跳效果
        val pageFlowList: List[(Long, Long)] = userVisitList.zip(userVisitList.tail)
        //2.5对拉链之后的集合机构进行转换
        pageFlowList.map {
          case (pageA, pageB) => {
            (pageA + "_" + pageB, 1)
          }
        }
      }
    )
    //2.6 对上面RDD的结构进行转换，只保留页面跳转以及计数 (pageA_pageB,1)
    val pageFlowRDD: RDD[(String, Int)] = mapValueRDD.map(_._2).flatMap(list=>list)

    //2.7 对页面跳转情况进行汇总 (pageA_pageB,100)
    val reduceRDD: RDD[(String, Int)] = pageFlowRDD.reduceByKey(_+_)

    //3.计算页面单跳转换率
    reduceRDD.foreach{
      case (pageAToPageB,fz)=>{
        //3.1获取pageA页面的id
        val pageAId: String = pageAToPageB.split("_")(0)
        //3.2根据pageA页面id到fmMap集合中获取pageA页面的总访问量
        val fm: Int = fmMap.getOrElse(pageAId.toLong,1)
        println(pageAToPageB + "------>" + fz.toDouble / fm)
      }
    }


    //关闭资源
    sc.stop()

  }

}
