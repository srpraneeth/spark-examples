package org.spark.examples.core

import java.util

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoders, SQLContext, SparkSession}

import scala.reflect.ClassTag
import org.apache.spark.{SparkConf, SparkContext}
import org.spark.examples.core.RDDMain._

object Main extends App {

  val conf = new SparkConf().setAppName("Core-Example").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  //RDD Operations

  //  val rddTeams = getRDD[Team]("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Team.csv", team => {
  val rddTeams = getRDD[Team]("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Team.csv", teamString => {
    val tokens = teamString.split(",")
    Team(tokens(0).toInt, tokens(1), tokens(2))
  })

  //  val rddMatch = getRDD[Match]("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Match.csv", matchString => {
  val rddMatch = getRDD[Match]("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Match.csv", matchString => {
    val tokens = matchString.split(",")
    Match(tokens(0).toInt, tokens(1), tokens(2).toInt, tokens(3).toInt, tokens(4).toInt, tokens(5))
  })

  //  val rddBall = getRDD[Ball]("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Ball.csv", ball => {
  val rddBall = getRDD[Ball]("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Ball_by_Ball.csv", ballString => {
    val tokens = ballString.split(",")
    Ball(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, tokens(3).toInt, tokens(4).toInt, tokens(5).toInt, tokens(6).toInt, tokens(8).toInt, tokens(9).toInt,
      getSafeInt(tokens(10)), tokens(11), getSafeInt(tokens(12)), getSafeInt(tokens(13)), tokens(14), getSafeInt(tokens(15)))
  })

  val rddMatchId = rddMatch.map(eachMatch => (eachMatch.teamId, eachMatch))
  val rddTeamId = rddTeams.map(eachTeam => (eachTeam.id, eachTeam))
  val joinedRddMatchTeam = rddMatchId.join(rddTeamId)
  val rddTeamMatchJoined = joinedRddMatchTeam.map(eachJoinRec => (eachJoinRec._2._1, eachJoinRec._2._2))


  // SQL - DF Operations
  val sparkSession = SparkSession.builder().config(conf).getOrCreate()

  import sparkSession.implicits._
  val ballDf = rddBall.toDF
  ballDf.show(2)
  ballDf.printSchema
  ballDf.createOrReplaceTempView("Ball")

  val result = sparkSession.sql("SELECT strikerId, count(strikerId) FROM Ball where bowlerId = 14 and scored = 6 group by strikerId")
  result.show(20)

  ballDf.select(s"strikerId").show(10)
  ballDf.select(s"strikerId", s"scored").show(10)


  val playerDf = sparkSession.read.option("header", "true").csv("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Player.csv")
  playerDf.show(10)
  playerDf.printSchema()
  playerDf.select(s"Player_Name").show
  playerDf.select(s"Player_Name", s"Country").where(s"Country = 'India'").show
  playerDf.select(s"Player_Name", s"Country").groupBy(s"Country").count.as(s"NoOfPlayers").orderBy(s"count").show

  playerDf.createOrReplaceTempView("PLAYERS_TABLE")
  sparkSession.sql("SELECT Player_Name FROM PLAYERS_TABLE WHERE COUNTRY = 'India'").show
  sparkSession.sql("SELECT Country, count(Player_Name) as NoOfPlayers FROM PLAYERS_TABLE GROUP BY Country ORDER BY NoOfPlayers DESC").show

//  sparkSession.newSession().sql("SELECT * FROM PLAYERS_TABLE").show
  playerDf.createGlobalTempView("PLAYERS_TABLE")
  sparkSession.newSession().sql("SELECT * FROM global_temp.PLAYERS_TABLE").show

  //DataSets
  val playerDS = playerDf.as[Player]
  playerDS.show

  playerDf.select(s"Player_Name", s"Country").write.format("csv").save("NameCountry.csv")






  def getRDD[A](file:String, mapString:String => A)(implicit classTag: ClassTag[A]) = {
    val rdd = sc.textFile(file)
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




  /*def getMatchRDD: RDD[Match] = {
//    val rddMatch = sc.textFile("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Match.csv")
    val rddMatch = sc.textFile("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Match.csv")

    val rddMatchCleaned = rddMatch.mapPartitionsWithIndex (removeHeaderRow)
    val mapToMatch = { matchString: String =>
      val tokens = matchString.split(",")
      Match(tokens(0).toInt, tokens(1), tokens(2).toInt, tokens(3).toInt, tokens(4).toInt, tokens(5))
    }
    val rddMatchTransformed = rddMatchCleaned.map(mapToMatch)
    rddMatchTransformed
  }



  def getTeamsRDD: RDD[Team] = {
//    val rddTeams = sc.textFile("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Team.csv")
    val rddTeams = sc.textFile("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Team.csv")
    rddTeams.count

    val rddTeamsCleaned = rddTeams.mapPartitionsWithIndex(removeHeaderRow)
    rddTeamsCleaned.count

    val rddTeamTransformed = rddTeamsCleaned.map { team: String =>
      val tokens = team.split(",")
      Team(tokens(0).toInt, tokens(1), tokens(2))
    }
    rddTeamTransformed.count
//    rddTeamTransformed.filter { team => team.id.equals(2) }.foreach(println(_))
    rddTeamTransformed
  }

  def getBallByBallRDD: RDD[Ball] = {
//    val rddBall = sc.textFile("hdfs://1be6d737776c:9000/indian-premier-league-csv-dataset/Ball_by_Ball.csv")
    val rddBall = sc.textFile("file:///Users/pramesh/Downloads/indian-premier-league-csv-dataset/Ball_by_Ball.csv")
    val rddBallCleaned = rddBall.mapPartitionsWithIndex(removeHeaderRow)
    rddBallCleaned.count
    val rddBallTransformed = rddBallCleaned.map { ballString: String =>
      val tokens = ballString.split(",")
      Ball(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, tokens(3).toInt, tokens(4).toInt, tokens(5).toInt, tokens(6).toInt, tokens(8).toInt, tokens(9).toInt,
        getSafeInt(tokens(10)), tokens(11), getSafeInt(tokens(12)), getSafeInt(tokens(13)), tokens(14), getSafeInt(tokens(15)))
    }
    rddBallTransformed.count
    rddBallTransformed
  }*/

}

//case class Team(id: Int, name: String, code: String)
//case class Match(id: Int, date: String, teamId: Int, opponentId:Int, seasonId: Int, venue: String)
case class Ball(matchId: Int, inningsId: Int, overId: Int, ballId: Int, battingTeamId: Int, bowlingTeamId: Int, strikerId: Int, nonStrikerId: Int, bowlerId: Int,
                scored:Option[Int], extraType: String, extraRuns: Option[Int], playerDismissed: Option[Int], playerDismissalType: String, fielderId: Option[Int])
case class Player(Player_Id: String, Player_Name:String, dob:String, Batting_Hand:String, Bowling_Skill: String, Country:String, Is_Umpire:Boolean)
