package org.spark.examples.streaming

object StreamingUtils {

  def getRun(str: String) = if(str.trim == "") 0 else str.trim.toInt

  def getOutOrNot(str: String) = if(str.trim == "") 0 else 1

}
