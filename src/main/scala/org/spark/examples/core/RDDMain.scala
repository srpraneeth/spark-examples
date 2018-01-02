package org.spark.examples.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.spark.examples.core.utils.RDDUtils._

/**
  * Use case is to join the Match and Team csv to get the joined RDD for
  * each match home team and the team details of the home team and
  * perform following operations on that
  * 1) Count
  * 2) Print
  * 3) Filter
  * 4) Map
  * 5) Group
  * 6) Reduce
  * 7) Sort
  * 8) Take
  * 9) Save Results
  *
  */
object RDDMain extends App {

  val conf = new SparkConf setAppName("RDD-Example") setMaster("local[2]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  // RDD Operations

  // Reading the Team CSV files to create a RDD of Teams.
  // Actual reading of the file does not happen at this point, only when a driver operation is called the actual file is read.
  // val rddTeams = getRDD[Team]("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Team.csv", team => {
//  val rddTeams = getRDD[Team](sc, "file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Team.csv", teamString => {
  val rddTeams = getRDD[Team](sc, "./data/indian-premier-league-csv-dataset/Team.csv", teamString => {
    val tokens = teamString split(",")
    Team(tokens(0) toInt, tokens(1), tokens(2))
  })
  // Persisting the RDD in the spark distributed memory, this eliminates file reads again when the RDD is read (the trade of would be it consumes memory)
  // Since this is a small data set storing inside the memory
  rddTeams.persist(StorageLevel.MEMORY_ONLY)

  // Reading the Matches CSV file to create a RDD of Matches
  //  val rddMatches = getRDD[Match]("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Match.csv", matchString => {
  val rddMatches = getRDD[Match](sc, "./data/indian-premier-league-csv-dataset/Match.csv", matchString => {
    val tokens = matchString.split(",")
    Match(tokens(0).toInt, tokens(1), tokens(2).toInt, tokens(3).toInt, tokens(4).toInt, tokens(5))
  })

  // Create the PairRDD with TeamId and Team for Joining
  val rddTeamId = rddTeams.map { eachTeam => (eachTeam.id, eachTeam) }

  // Create the PairRDD with TeamId and Match for Joining with TeamId
  val rddMatchesId = rddMatches.map { eachMatch => (eachMatch.teamId, eachMatch) }

  // Join the PairRDD of Team with PairRDD of Matches
  val joinedRddMatchesTeam = rddMatchesId join rddTeamId

  // Converting the PairRDD
  val rddTeamMatchJoined = joinedRddMatchesTeam.map { eachJoinRec => (eachJoinRec._2._1, eachJoinRec._2._2) }

  // Count the No of items in the RDD
  val countOfRecordsInRDD = rddTeamMatchJoined.count
  println(countOfRecordsInRDD)

  // Printing the Contents of the RDD
  rddTeamMatchJoined.foreach { println }

  // Filters the RDD for matches played by RCB
  rddTeamMatchJoined.filter { _._2.code == "RCB" }.foreach { println }

  // Maps each value in the RDD to a different type
  rddTeamMatchJoined.map { matchTeam => TeamMatch(matchTeam._2, matchTeam._1) }.foreach { println }

  // Groups the RDD to find the Total no of matches played by each Team
  rddTeamMatchJoined
    .groupBy(eachJoin => eachJoin._2.name)
    .map { eachGroup => ( eachGroup._1, eachGroup._2.size ) }
    .foreach { println }

  // Maps RDD to another RDD of key (teamId, opponentId) and value 1 and
  // then reduces the RDD to count the total no of matches played by the two teams
  rddTeamMatchJoined
    .map { item => ( (item._2.id, item._1.opponentId), 1) }
    .reduceByKey{ _ + _ }.foreach { println }

  // Maps RDD to another RDD ok Key Team code and value Match&Team and
  // sort the RDD by Key in descending order
  rddTeamMatchJoined.map { eachJoin => (eachJoin._2.code, eachJoin) }
      .sortByKey(false).map{_._2}.foreach { println }

  // Takes only 10 Records from RDD
  rddTeamMatchJoined take(10)

  // Save the RDD to the file. This can be a HDFS file location also.
  // This can be a hdfs location also.
  // rddTeamMatchJoined.saveAsTextFile("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Result/Match&Team.data")
  // RDD can be converted to DataFrames and then saved as Parquet files also.
  rddTeamMatchJoined saveAsTextFile("./data/Result/Match&Team.data")

  case class Team(id: Int, name: String, code: String)
  case class Match(id: Int, date: String, teamId: Int, opponentId:Int, seasonId: Int, venue: String)
  // matchPlayed is used here since match is key word in scala
  case class TeamMatch(team:Team, matchPlayed:Match)

}