package com.atguigu.spark.day08

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Test {

  def main(args: Array[String]): Unit = {


    import scala.collection.mutable.ListBuffer

    /**
     * Author talent2333
     * Date 2020/7/15 9:39
     * Description
     */

        val ints: List[Int] = List(1,2,3,4,5,6)
        val iterator: Iterator[List[Int]] = ints.sliding(2,1)

    val ints1 = Array(1, 2, 3, 4, 5, 6)
    val iterator1 = ints1.sliding(2, 1)

        /*for (elem <- ints.sliding(3, 3)) {
          println(elem)
        }*/
        //iterator.foreach(println)
        //val list: List[List[Int]] = iterator.toList


        while (iterator.hasNext){
          val ints1 = iterator.next()
          println(ints1)
        }
        println("============")
        //    val buffer = new ListBuffer[List[Int]]
        //    while (iterator.hasNext){
        //      val it: List[Int] = iterator.next()
        //      buffer+=it
        //    }
        //    buffer+=List(1,2,3)
        //    buffer+=List(4,5,6)
        //    buffer.foreach(println)
      }


}
