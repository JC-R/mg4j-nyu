package edu.nyu.tandon.experiments.ml.StaticPruning

import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.apache.spark.sql._

import scala.collection.mutable

/**
  * Created by juan on 12/28/16.
  */
object TrainCV {

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      System.err.println("Usage: Train <trainFile> <outDir> <nfolds> <nrounds> <maxDepth> <nthread> <task>")
      System.exit(1)
    }

    val trainFile = args(0)
    val outDir = args(1)
    val nfolds = args(2).toInt
    val nrounds = args(3).toInt
    val maxDepth = args(4).toInt
    val nthreads = args(5).toInt
    val task = args(6)

    val spark = SparkSession
      .builder()
      .appName("MLTrain")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val trainMat = new DMatrix(trainFile)

    val params = new mutable.HashMap[String, Any]()
    params += "eta" -> 0.02
    params += "alpha" -> 0.1
    params += "objective" -> "reg:linear"
    params += "booster" -> "gbtree"
    params += "min_child_weight" -> 4
    params += "max_depth" -> maxDepth
    params += "silent" -> 0
    params += "tree_method" -> "approx"
    params += "save_period" -> 50
    params += "seed" -> 12345
    params += "task" -> task
    params += "nthread" -> 6

    val round: Int = nrounds
    val nfold: Int = nfolds

    val metrics: Array[String] = null

    val evalHist: Array[String] =
      XGBoost.crossValidation(trainMat, params.toMap, round, nfold, metrics, null, null)

    evalHist.foreach(e => System.out.print(e))
  }

}
