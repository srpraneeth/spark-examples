package org.spark.examples.core.utils

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

object RDDUtils {

  def getRDD[A](sc:SparkContext, file:String, mapString:String => A)(implicit classTag: ClassTag[A]) = {
    val rdd = sc.textFile(file)
    val rddCleaned = rdd.mapPartitionsWithIndex(removeHeaderRow)
    val rddTransformed = rddCleaned.map(mapString)
    rddTransformed
  }

  def getSafeInt(str: String): Option[Int] = {
    if(!str.trim.isEmpty)
      Option(str.trim toInt)
    else None
  }

  def removeHeaderRow = (i:Int, itr:Iterator[String]) =>
    if (i == 0 && itr.hasNext) {
      itr.next
      itr
    } else itr

}
