package org.spark.examples.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.spark.examples.core.utils.RDDUtils._

object RDDMain extends App {


  val conf = new SparkConf setAppName("RDD-Example") setMaster("local[2]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  // RDD Operations

  // Reading the Team CSV files to create a RDD of Teams.
  // Actual reading of the file does not happen at this point, only when a driver operation is called the actual file is read.
  // val rddTeams = getRDD[Team]("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Team.csv", team => {
  val rddTeams = getRDD[Team](sc, "file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Team.csv", teamString => {
    val tokens = teamString split(",")
    Team(tokens(0) toInt, tokens(1), tokens(2))
  })
  // Persisting the RDD in the spark distributed memory, this eliminates file reads again when the RDD is read (the trade of would be it consumes memory)
  // Since this is a small data set storing inside the memory
  rddTeams.persist(StorageLevel.MEMORY_ONLY)

  // Reading the Matches CSV file to create a RDD of Matches
  //  val rddMatches = getRDD[Match]("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Match.csv", matchString => {
  val rddMatches = getRDD[Match](sc, "file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Match.csv", matchString => {
    val tokens = matchString.split(",")
    Match(tokens(0).toInt, tokens(1), tokens(2).toInt, tokens(3).toInt, tokens(4).toInt, tokens(5))
  })

  val rddMatchId = rddMatches.map(eachMatch => (eachMatch.teamId, eachMatch))
  val rddTeamId = rddTeams.map(eachTeam => (eachTeam.id, eachTeam))
  val joinedRddMatchTeam = rddMatchId.join(rddTeamId)
  val rddTeamMatchJoined = joinedRddMatchTeam.map(eachJoinRec => (eachJoinRec._2._1, eachJoinRec._2._2))




}

case class Team(id: Int, name: String, code: String)
case class Match(id: Int, date: String, teamId: Int, opponentId:Int, seasonId: Int, venue: String)
case class Ball(matchId: Int, inningsId: Int, overId: Int, ballId: Int, battingTeamId: Int, bowlingTeamId: Int, strikerId: Int, nonStrikerId: Int, bowlerId: Int,
                scored:Option[Int], extraType: String, extraRuns: Option[Int], playerDismissed: Option[Int], playerDismissalType: String, fielderId: Option[Int])
case class Player(Player_Id: String, Player_Name:String, dob:String, Batting_Hand:String, Bowling_Skill: String, Country:String, Is_Umpire:Boolean)




