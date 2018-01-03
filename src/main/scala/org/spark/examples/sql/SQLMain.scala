package org.spark.examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.spark.examples.core.utils.RDDUtils._

/**
  * Use case is to Read the BallByBall and Players dataset and apply following sql operations on those.
  * 1) print
  * 2) printSchema
  * 3) Select
  * 4) Where
  * 5) GroupBy
  * 6) OrderBy
  * 7) Alias
  * 8) Temp and Global Views
  * 9) Joins
  */
object SQLMain extends App {

  val conf = new SparkConf().setAppName("Core-Example").setMaster("local")
  val sparkSession = SparkSession.builder().config(conf).getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")

  // SQL - DF Operations

  // Convert a RDD to a DF
  // Creating a RDD of BallByBall CSV
  val rddBallByBall = getRDD[Ball](sparkSession.sparkContext, "./data/indian-premier-league-csv-dataset/Ball_by_Ball.csv", ballString => {
    val tokens = ballString.split(",")
    Ball(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, tokens(3).toInt, tokens(4).toInt, tokens(5).toInt, tokens(6).toInt, tokens(8).toInt, tokens(9).toInt,
      getSafeInt(tokens(10)), tokens(11), getSafeInt(tokens(12)), getSafeInt(tokens(13)), tokens(14), getSafeInt(tokens(15)))
  })

  // Implicits import that are required for Spark SQL
  // https://docs.scala-lang.org/tour/implicit-parameters.html
  import sparkSession.implicits._

  // Converting an RDD to DF
  // toDF comes from sql.DataFrame package, this is available due to implicit imports
  val ballDF = rddBallByBall.toDF

  // Reading a CSV file to create a DF
  // Header = true options indicate that the first row in the csv is its header
  val playerDf = sparkSession.read.option("header", "true").csv("./data/indian-premier-league-csv-dataset/Player.csv")

  // Print the DF
  playerDf.show

  // Print the Schema of the DF
  playerDf.printSchema

  // Select the Columns in the DF
  playerDf.select(s"Player_Name", s"Country").show

  // Filter the DF by Where condition
  playerDf.select(s"Player_Name", s"Country").where(s"Country = 'India'").show

  // Aggregate by Count and Order DF by Count
  playerDf.select(s"Player_Name", s"Country").groupBy(s"Country").count.orderBy(s"count").show

  // This registers the DF as a Temp Table on which the SQL can be fired
  playerDf.createOrReplaceTempView("Player")

  sparkSession.sql("SELECT * FROM PLAYER").show
  sparkSession.sql("SELECT PLAYER_NAME as Name, COUNTRY as HomeTeam FROM PLAYER WHERE COUNTRY = 'India'").show
  sparkSession.sql("SELECT Country as HomeTeam, COUNT(COUNTRY) as NoOfPlayers FROM PLAYER GROUP BY COUNTRY ORDER BY NoOfPlayers DESC").show

  // CreateOrReplaceTempView Only is local to the Current Spark Session
  // Below line will throw a Exception (Table or view not found: PLAYER;)
//  sparkSession.newSession().sql("SELECT * FROM PLAYER").show

  // In order to make a table available across sessions use
  playerDf.createOrReplaceGlobalTempView("Player")
  sparkSession.newSession().sql("SELECT * FROM global_temp.PLAYER").show

  ballDF.createOrReplaceTempView("Ball")

  // Example to show a complex query with join groupby orderby and filter
  // This joins the player and ball dataset to find the players with most Boundaries(6Runs or 4Runs) in desc order with the no of boundaries scored by them in each match
  sparkSession.sql("SELECT b.matchId AS MatchNo, p.Player_Name AS Player, count(b.scored) AS NoOfBoundaries, b.scored AS 6RunsOr4Runs " +
    "FROM BALL b JOIN PLAYER p WHERE p.Player_Id = b.strikerId AND b.scored IN (4,6) GROUP BY p.Player_Name, b.matchId, b.scored ORDER BY NoOfBoundaries DESC ").show



  case class Ball(matchId: Int, inningsId: Int, overId: Int, ballId: Int, battingTeamId: Int, bowlingTeamId: Int, strikerId: Int, nonStrikerId: Int, bowlerId: Int,
                  scored:Option[Int], extraType: String, extraRuns: Option[Int], playerDismissed: Option[Int], playerDismissalType: String, fielderId: Option[Int])
  case class Player(Player_Id: String, Player_Name:String, dob:String, Batting_Hand:String, Bowling_Skill: String, Country:String, Is_Umpire:Boolean)


}
