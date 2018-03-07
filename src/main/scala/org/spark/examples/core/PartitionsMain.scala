package org.spark.examples.core

import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object PartitionsMain extends App {

  // Creating the Spark Session
  val sparkSession = SparkSession.builder().master("local[2]").appName("Structured-Streaming").getOrCreate
  import sparkSession.implicits._
  sparkSession.sparkContext.setLogLevel("WARN")

  val matchDataDF = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("./data/indian-premier-league-csv-dataset/Match.csv")
    .withColumn("Won_By", 'Won_By.cast("Int"))
    .select($"Team_Name_Id" as "Team1", $"Opponent_Team_Id" as "Team2", $"Venue_Name" as "Venue",
      $"Toss_Winner_Id" as "Toss Winner", $"Toss_Decision" as "Toss Decision", $"Win_Type" as "Win Type",
      $"Won_By" as "Won By", $"City_Name" as "City", $"Match_Winner_Id" as "Winner")

  var indexers: Array[StringIndexer] = Array()
  indexers = indexers :+ new StringIndexer().setInputCol("Venue").setOutputCol("Venue" + "_indexed")
  indexers = indexers :+ new StringIndexer().setInputCol("Toss Decision").setOutputCol("Toss Decision" + "_indexed")
  indexers = indexers :+ new StringIndexer().setInputCol("Win Type").setOutputCol("Win Type" + "_indexed")
  indexers = indexers :+ new StringIndexer().setInputCol("City").setOutputCol("City" + "_indexed")

  var encoders: Array[OneHotEncoder] = Array()
  encoders = encoders :+ new OneHotEncoder().setInputCol("Venue" + "_indexed").setOutputCol("Venue_encoded")
  encoders = encoders :+ new OneHotEncoder().setInputCol("Toss Decision" + "_indexed").setOutputCol("Toss Decision" + "_encoded")
  encoders = encoders :+ new OneHotEncoder().setInputCol("Win Type" + "_indexed").setOutputCol("Win Type_encoded")
  encoders = encoders :+ new OneHotEncoder().setInputCol("City" + "_indexed").setOutputCol("City_encoded")

  val pipeline = new Pipeline().setStages(indexers ++ encoders)
  val encodedMatchDataDF = pipeline
    .fit(matchDataDF)
    .transform(matchDataDF)

  matchDataDF.printSchema()
  
  val featureColumnNames = Array("Team1", "Team2", "Venue_encoded",
    "Toss Winner", "Toss Decision_encoded", "Win Type_encoded",
    "Won By", "City_encoded", "Winner")
  val vectorAssembler = new VectorAssembler().setInputCols(featureColumnNames).setOutputCol("features")
  val featureVectorizedMatchDataDF = vectorAssembler.transform(encodedMatchDataDF.na.drop)

  featureVectorizedMatchDataDF.printSchema()

  val featureVectorizedSelectedMatchDataDF = featureVectorizedMatchDataDF.withColumnRenamed("Winner", "label")
                                                  .map(row => LabeledPoint(row.getInt(8).toDouble, row(17).asInstanceOf[Vector]))
  val traintestDF = featureVectorizedSelectedMatchDataDF.randomSplit(Array(0.9, 0.1))

  traintestDF(0).printSchema
  traintestDF(0).show

//  val lr = new LinearRegression()
//    .setMaxIter(10)
//    .setRegParam(0.3)
//    .setElasticNetParam(0.8)
//  val lrModel = lr.fit(v(0))
//
//  println(lrModel.numFeatures)
//
//  v(1).show
//
//  val result = lrModel.evaluate(v(1))
//
//  val toDoubleUDF = functions.udf({ double:Double => double.round })
//
//  result.predictions.withColumn("predictions", toDoubleUDF(result.predictions("prediction")))
//      .select($"Team1", $"Team2", $"Venue", $"Toss Winner", $"Toss Decision", $"Win Type", $"Won By", $"City", $"label" as "Winner", $"predictions")
//      .show
//
//  val newData = Seq("335996,25-Apr-08,4,7,1,Punjab Cricket Association Stadium, Mohali,7,field,0,1,0,by runs,66,4,26,472,492,Chandigarh,India")
//    .toDF("Match_Id","Match_Date","Team_Name_Id","Opponent_Team_Id","Season_Id","Venue_Name","Toss_Winner_Id","Toss_Decision","IS_Superover","IS_Result","Is_DuckWorthLewis","Win_Type","Won_By","Match_Winner_Id","Man_Of_The_Match_Id","First_Umpire_Id","Second_Umpire_Id","City_Name","Host_Country")
//  pipeline
//    .fit(newData)
//    .transform(newData).show

  val classifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043)
  val model = classifier.fit(traintestDF(0))

  val prediction = model.transform(traintestDF(1))
  prediction.show

}