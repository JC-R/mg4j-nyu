package edu.nyu.tandon.experiments.staticpruning.ml.xgboost

import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by juan on 1/20/17.
  */
object ModelTune {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ModelTune").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    if (args.length < 12) {
      System.err.println("\nUsage: ModelTune <Features> <label> <startDepth> <endDepth> <depthInc> <startRound> <endRound> <roundInc> <startEta> <endEta> <eta Inc.> <extMem>\n")
      System.exit(1)
    }

    spark.sparkContext.setLogLevel("ERROR")

    // parse training file to data frame
    val train = spark.read.parquet(args(0))
    val label = args(1)

    val startDepth = args(2).toInt
    val endDepth = args(3).toInt
    val depthInc = args(4).toInt

    val startRound = args(5).toInt
    val endRound = args(6).toInt
    val roundInc = args(7).toInt

    val startEta = args(8).toDouble
    val endEta = args(9).toDouble
    val etaInc = args(10).toDouble

    val extMem = args(11).toBoolean

    // prediction
    val params = new mutable.HashMap[String, Any]()
    params += "eta" -> startEta
    params += "max_depth" -> startDepth
    params += "silent" -> 0
    params += "ntreelimit" -> 1000
    params += "objective" -> "reg:linear"
    params += "subsample" -> 1
    params += "num_round" -> startRound
    params += "nthread" -> 1
    params += "useExternalMemory" -> extMem
    params += "early.stop.round" -> 3

    val xgbEstimator = new XGBoostEstimator(params.toMap)
      .setLabelCol(label)
      .setFeaturesCol("features")

    // create param ranges to search
    val paramGrid = new ParamGridBuilder()
      .addGrid(xgbEstimator.round, (startRound to endRound by roundInc).toArray)
      .addGrid(xgbEstimator.eta, (startEta to  endEta by etaInc).toArray)
      .addGrid(xgbEstimator.maxDepth, (startDepth to endDepth by depthInc).toArray)
      .build()

    val tv = new TrainValidationSplit()
      .setEstimator(xgbEstimator)
      .setEvaluator(new RegressionEvaluator().setLabelCol(label))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.75)

    val bestModel = tv.fit(train)

    println("best model: ")
    bestModel.get(bestModel.estimator).get.extractParamMap().toSeq.foreach(pp => println(pp.toString))

  }
}
