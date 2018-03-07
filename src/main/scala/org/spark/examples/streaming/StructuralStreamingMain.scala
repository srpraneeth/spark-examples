package org.spark.examples.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Use case is to calculate the Running batting average of the Player using spark structured streaming and join that with the static data.
  *
  */
object StructuralStreamingMain extends App {

  // Creating the Spark Session
  val sparkSession = SparkSession.builder().master("local[2]").appName("Structured-Streaming").getOrCreate
  import sparkSession.implicits._
  sparkSession.sparkContext.setLogLevel("WARN")

  // Reading the Static Data
  val playersDS = sparkSession.read.option("header", "true").csv("./data/indian-premier-league-csv-dataset/Player.csv")
    .select($"Player_Id" as "playerId", $"Player_Name" as "playerName", $"Country" as "country")
    .selectExpr("cast(playerId as int)", "playerName","country")
    .as[Player]

  // Reading the Streaming Data with interface similar to SQL
  val lines = sparkSession.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load.as[String]

  // Incoming Streaming data is mapped as the Ball DataSet
  val ballDF = lines.map { ballString =>
    val tokens = ballString.split(",")
//    Ball(tokens(0).toInt, tokens(1).toInt, getOutOrNot(tokens(2)))
    Ball(tokens(6).toInt, tokens(10).toInt, getOutOrNot(tokens(13)))

  }

  // With SQL
  // Grouping the incoming Streaming Ball DataSet by playerId and Summing the Total Runs and Total Outs of each player
  val ballDFGrouped = ballDF.groupBy($"playerId").agg(sum($"runs") as "totalRuns", sum($"outs") as "totalOuts")

  // Selecting each player id and the calculating the average
  val ballDFGroupedSelect = ballDFGrouped.select($"playerId", ($"totalRuns" / $"totalOuts") as "avg")

  // Joining each of the average with the static dataset of players
  val ballDFGroupedSelectWithPlayer = ballDFGroupedSelect.join(playersDS, ballDFGroupedSelect.col("playerId") === playersDS.col("playerId"))

  val ballDFGroupedSelectWithPlayerWithSelect = ballDFGroupedSelectWithPlayer.select(ballDFGroupedSelect.col("playerId") as "ID", $"playerName" as "Player Name", $"country" as "Country", $"avg" as "Batting Average")

  // Outputting the Result every time to console
  // Output modes can be complete, append, update
  val query = ballDFGroupedSelectWithPlayerWithSelect.writeStream.outputMode("complete").format("console").start

  // This is to see the metrics about the Continuous Query Running
//  new Thread {
//    override def run = {
//      while(true) {
//        println(query.id)
//        println(query.name)
//        println(query.isActive)
//        println(query.explain)
//        println(query.exception)
//        println(query.lastProgress)
//        println(query.recentProgress)
//        println(query.runId)
//        println(query.status)
//        println(query)
//        Thread sleep 20000
//      }
//    }
//  } start

  // Blocks the current thread
  query awaitTermination

  case class Ball(playerId:Int, runs: Int, outs:Int)

  case class Player(playerId:Int, playerName:String, country:String)

  def getOutOrNot(str: String) = if(str.trim == "") 0 else 1

}