package edu.nyu.tandon.experiments.ml.StaticPruning

import scala.collection.mutable
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import ml.dmlc.xgboost4j.scala.spark.DataUtils
import org.apache.spark
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql._
import org.apache.spark.rdd._

/**
  * Created by juan on 1/20/17.
  */
object CV {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TrainDF")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    if (args.length < 7) {
      System.err.println("\nUsage: CV <TrainFeatures> <label> <folds> <rounds> <eta> <maxDepth> <extMem>\n")
      System.exit(1)
    }

    val featuresFile = args(0)
    val label = args(1)
    val nFolds = args(2).toInt
    val nRounds = args(3).toInt
    val eta = args(4).toDouble
    val maxDepth = args(5).toInt
    val extMem = args(6).toBoolean

    import spark.implicits._

    val df = spark.read.parquet(args(0)).na.fill(0)
    val lp = df.map(row => LabeledPoint(row.getAs[Long](label).toDouble, row.getAs[Vector]("features")))

    val iter: Iterator[LabeledPoint]  = lp.rdd.toLocalIterator

    implicit val sc = spark.sparkContext

    val xp = DataUtils.fromSparkPointsToXGBoostPoints(iter)
    val trainMat: DMatrix = new DMatrix(xp)

    // set params
    val params = new mutable.HashMap[String, Any]

    params.put("eta", eta)
    params.put("max_depth", maxDepth)
    params.put("silent", 0)
    params.put("nthread", 6)
    params.put("objective", "reg:linear")
    params.put("gamma", 1.0)

    // do 5-fold cross validation
    val round: Int = nRounds
    val nfold: Int = nFolds

    // set additional eval_metrics
    val metrics: Array[String] = null

    val evalHist: Array[String] =
      XGBoost.crossValidation(trainMat, params.toMap, round, nfold, metrics, null, null)

    evalHist.foreach(e => println(e))
  }
}
