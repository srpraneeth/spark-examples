package org.spark.examples.core

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.reflect.ClassTag

object StreamingMain extends App {

  val sparkConf = new SparkConf().setAppName("Streaming-Examples").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint("./checkpoint/storage")
  ssc.sparkContext.setLogLevel("WARN")

  def getRun(str: String) = {
    if (str.trim == "") 0 else str.trim.toInt
  }

  def getOutOrNot(str: String) = {
    if(str.trim == "") 0 else 1
  }

  val rddInitialBall = getRDD[(Int, (Int, Int))]("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Ball_by_Ball.csv", ballString => {
    val tokens = ballString.split(",")
    (tokens(6).toInt, ( getRun(tokens(10)), getOutOrNot(tokens(13)) ))
  }).reduceByKey((tuple1:(Int,Int), tuple2:(Int,Int)) => (tuple1._1 + tuple2._1 , tuple1._2 + tuple2._2))
  rddInitialBall.filter(ball => ball._1.equals(2)).foreach { println }

  val newBallsStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)
  val newPlayerRunsOutStream = newBallsStream.map { newBall => {
    val tokens = newBall.split(",")
    (tokens(6).toInt, ( getSafeInt(tokens(10)).getOrElse(0), getSafeInt(tokens(13)).getOrElse(0) ))
  }}

  val mappingFunc = (playerId:Int, one:Option[(Int, Int)], state:State[(Int, Int)]) => {
    val newScore = one.getOrElse((0,0))
    val existingState = state.getOption().getOrElse((0,0))
    val newState = (newScore._1 + existingState._1 , newScore._2 + existingState._2)
    state.update(newState)
    (playerId, newState)
  }

  val newPlayerRunsOutStreamWithInitialState = newPlayerRunsOutStream.mapWithState(StateSpec.function(mappingFunc).initialState(rddInitialBall))
  val transformed = newPlayerRunsOutStreamWithInitialState.map { eachPlayer:(Int, (Int, Int)) =>
    (eachPlayer._1, (eachPlayer._2._1 / eachPlayer._2._2))
  }.transform{ eachRdd => eachRdd.sortBy(_._2, false) }
  transformed.print(5)
//
//  val red = (scored:Option[Int], scoredAccumulated:Option[Int]) => {
//    Option(scored.getOrElse(0) + scoredAccumulated.getOrElse(0))
//  }


  //  val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)
//  val words = lines.flatMap { line => line.split(" ")}
//  val wordTuples = words.map { word => (word, 1) }
//
//  val mappingFunc = (word:String, one:Option[Int], state: State[Int]) => {
//    println(s"Word: $word , State: ${state.getOption}, One: $one")
//    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
//    val output = (word, sum)
//    state.update(sum)
//    output
//  }

//  val statefulWordTuples = wordTuples.mapWithState(
//    StateSpec.function(mappingFunc)
//      .initialState(ssc.sparkContext.parallelize(List(("Hello",1))))
//  )

//  val wordCount = wordTuples.reduceByKey(_ + _)
//  wordCount.print

//  statefulWordTuples.print
  ssc.start
  ssc.awaitTermination

  def getRDD[A](file:String, mapString:String => A)(implicit classTag: ClassTag[A]) = {
    val rdd = ssc.sparkContext.textFile(file)
    val rddCleaned = rdd.mapPartitionsWithIndex(removeHeaderRow)
    val rddTransformed = rddCleaned.map(mapString)
    rddTransformed
  }

  def getSafeInt(str: String): Option[Int] = {
    if(!str.trim.isEmpty) Option(str.trim.toInt) else None
  }

  def removeHeaderRow = { (i:Int, itr:Iterator[String]) =>
    if (i == 0 && itr.hasNext) {
      itr.next
      itr
    } else itr
  }

}


