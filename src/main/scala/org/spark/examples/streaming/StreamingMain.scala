package org.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.spark.examples.core.utils.RDDUtils._
import org.spark.examples.streaming.StreamingUtils._

/**
  * Use case is to calculate the Running batting average of the Player using spark streams.
  * In cricket, a player's batting average is the total number of runs they have scored divided by the number of times they have been out.
  * https://en.wikipedia.org/wiki/Batting_average
  */
object StreamingMain extends App {

  val LOCALHOST = "localhost"
  val PORT = 9999

  // Creating a new Spark StreamingContext with window interval of 5 seconds
  val sparkConf = new SparkConf()
    .setAppName("Player-Average-Score-Streaming")
    .setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  // Checkpoint is the storage for the intermediate results
  ssc.checkpoint("./checkpoint/storage")
  ssc.sparkContext.setLogLevel("WARN")

  // Read the Ball By Ball csv file and create an Initial PairRDD with player id as the key and
  // value is another tuple where key is total runs scored and value is no of times player has gone out
  val rddInitialBallByBall = getRDD[(Int, (Int, Int))](ssc.sparkContext, "./data/indian-premier-league-csv-dataset/Ball_by_Ball.csv", ballString => {
    val tokens = ballString.split(",")
    (tokens(6).toInt, (getRun(tokens(10)), getOutOrNot(tokens(13))))
  }).reduceByKey {
    (tuple1:(Int,Int), tuple2:(Int,Int)) => (tuple1._1 + tuple2._1 , tuple1._2 + tuple2._2)
  }
  // Printing the Initial RDD for debugging purpose
  rddInitialBallByBall.foreach { println }

  // Creating a Socket Stream from the localhost port 9999 from the StreamingContext created earlier
  // This could be any other type of stream like Kafka, Kinesis, Custom Stream etc
  val newBallByBallStream = ssc.socketTextStream(LOCALHOST, PORT, StorageLevel.MEMORY_AND_DISK)

  // Get the Player Id, runs scored and isPlayer out or not from  the input newBall data from the Str
  val newPlayerRunsOutStream = newBallByBallStream.map { newBall => {
    val tokens = newBall.split(",")
    if(tokens.size > 6)
      (tokens(6).toInt, ( getSafeInt(tokens(10)).getOrElse(0), getSafeInt(tokens(13)).getOrElse(0) ))
    else
      // Handle if any junk data coming from stream other than specified format
      (0, (0, 0))
  } }

  // Mapping the Stream with an initial State and a function that updates the current state of the player with the new data
  // mapWithState is marked @Experimental in spark-streaming:2.2.1, which means it may undergo changes in the future.
  val playerRunsOutStreamMappedWithInitialState = newPlayerRunsOutStream.mapWithState(StateSpec.function(stateMappingFunction).initialState(rddInitialBallByBall))

  // Calculating the Average Run rate of each of the player
  val playerRunRate = playerRunsOutStreamMappedWithInitialState.map { eachPlayer:(Int, (Int, Int)) => {
    if(eachPlayer._2._2 != 0)
      (eachPlayer._1, (eachPlayer._2._1 / eachPlayer._2._2 ))
    else
      (eachPlayer._1, (eachPlayer._2._1 / 1))
  }}.transform{ eachRdd => eachRdd.sortBy(_._2, false) }

  // Printing the player and the run rate of the player when the player average changes.
  // This can be sent to a persistance store like hive/ mysql to build real time dashboards
  playerRunRate.print()

  // Start the Streaming context to kickoff the processing
  ssc.start
  // Wait the current thread till the termination of the Streams
  // Any uncaught exceptions in the Stream is thrown in to this thread.
  ssc.awaitTermination


  /**
    * Mapping function that takes the Pair
    * @return
    * @see PairDStreamFunctions, Experimental
    *
    */
  def stateMappingFunction = (playerId:Int, newValue:Option[(Int, Int)], state:State[(Int, Int)]) => {
    // Getting the new Score if exists else a (0, 0)
    val newScore = newValue.getOrElse((0,0))
    // Getting the existing score from the existing state
    val existingState = state.getOption().getOrElse((0,0))
    // Computing the new State from that existing and new score of the player
    val newState = (newScore._1 + existingState._1 , newScore._2 + existingState._2)
    // Updating the existing score
    state.update(newState)
    // return the new tuple
    (playerId, newState)
  }

}