package edu.nyu.tandon.experiments.staticpruning.ml.xgboost

import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * Created by juan on 12/28/16.
  */
object TrainwLabel {

  def sqr(x: Float) = x * x

  def main(args: Array[String]): Unit = {

    if (args.length < 8) {
      System.err.println("\nUsage: TrainDF <train/eval> <rounds> <eta> <maxDepth> <extMem> <doEval> <silent> <label>\n")
      System.exit(1)
    }

    val featuresFile = args(0)
    val nRounds = args(1).toInt
    val eta = args(2).toDouble
    val maxDepth = args(3).toInt
    val extMem = args(4).toBoolean
    val doEval = args(5).toBoolean
    val silent = args(6).toInt
    val label = args(7)

    val spark = SparkSession
      .builder()
      .appName("TrainDF")
      .getOrCreate()

    import spark.implicits._

    println("Training ")
    spark.sparkContext.setLogLevel("ERROR")

    val params = new mutable.HashMap[String, Any]()
    params += "objective" -> "reg:linear"
    params += "booster" -> "gbtree"
    params += "max_depth" -> maxDepth
    params += "silent" -> silent
    params += "tree_method" -> "approx"
    params += "seed" -> 12345
    params += "ntreelimit" -> 1000
    params += "eta" -> eta
    params += "subsample" -> 1
    params += "nthread" -> 1
    params += "early.stop.round" -> 3

    // prediction
    val labels = Array("top10","top1k")

    implicit val sc = spark.sparkContext

    // read in features file; cleanup extra columns
    // do funky sampling to get enough non-zero values
    val train = spark.read.parquet(featuresFile+".train.parquet")
    val d1 = train.filter($"top10">0)
    val c = d1.count()
    val d2 = train.filter($"top10"===0)
    val ratio = if (c < d2.count) 1f * c / d2.count() else 1f
    val d3 = d2.sample(false,1f * c / d2.count())
    val d = d1.union(d3)

    println("Dataset rows: " + d.count())

    val eval = if (doEval) spark.read.parquet(featuresFile+".test.parquet")
        .sample(false, 0.2)
        .orderBy(desc(label))
    else spark.emptyDataFrame

    println("training: " + featuresFile + " - " + label)

    val booster = XGBoost.trainWithDataFrame(d.na.fill(0), params.toMap, nRounds, nWorkers = 1, useExternalMemory = extMem, labelCol = label, featureCol = "features", missing = 0f)
    booster.saveModelAsHadoopFile(featuresFile + "." + label + ".model")

    println("evaluating...")

    if (doEval) {
      val res = booster.transform(eval)
        .withColumn("e_top10", ($"top10" - $"prediction") * ($"top10" - $"prediction"))
        .withColumn("e_top1k", ($"top1k" - $"prediction") * ($"top1k" - $"prediction"))
        .withColumn("rse_top10", sqrt("e_top10"))
        .withColumn("rse_top1k", sqrt("e_top1k"))

      res.show(25)

      res.select("rse_" + label).describe().show()
    }
  }
}
