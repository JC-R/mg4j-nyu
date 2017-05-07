package edu.nyu.tandon.experiments.staticpruning.ml.h2o

/**
  * H2O MOJO Predictor
  *
  * Params: MOJO model, zipped
  * Dataset w/features, parquet
  *
  * Sample command line:
  * spark-submit <spark args> --packages ai.h2o:h20-genmodel:3.10.4.2 --class edu.nyu.tandon.staticpruning.ml.h2o.MojoPredict mg4j-nyu.jar gbm_top1k_all.zip cw09b.features.parquet
  *
  * Created by juan on 3/27/17.
  */

import _root_.hex.genmodel.MojoModel
import _root_.hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object MojoPredict {

  def toDouble: (Any) => Double = {
    case null => Double.NaN
    case d: Double => d
    case i: Int => i.toDouble
    case f: Float => f.toDouble
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("\nUse: MojoPredict <model> <dataset-parquet> <prediction>\n")
      System.exit(1)
    }

    val modelName = args(0)
    val fileName = args(1)

    val sparkConf = new SparkConf().setAppName("MoJoPredict")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // load the H2O model
    val m = MojoModel.load(modelName)
    val modelFeatures = m.getNames.slice(0, m.nfeatures())
    val model = new EasyPredictModelWrapper(m)

    val modelBroadcast = spark.sparkContext.broadcast(model)
    val features = spark.sparkContext.broadcast(modelFeatures)

    // load parquet data to runprediction on
    val df = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .parquet(fileName)

    // verify dataframe has all the required columns
    if ((modelFeatures diff df.columns).length != 0) {
      System.err.println("\n***ERROR: model and data field mismatch.\n")
      System.exit(1)
    }

    val d = df.map(r => {

      var row = new RowData
      features.value.foreach(f => row.put(f, new java.lang.Double(toDouble(r.get(r.fieldIndex(f))))))
      val prediction = modelBroadcast.value.predictRegression(row).value
      row.clear()
      row = null

      (r.getInt(r.fieldIndex("termID")), r.getInt(r.fieldIndex("docID")), prediction)

    }).toDF("termID", "docID", "predict").orderBy(desc("predict"))

    d.write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .parquet(args(2))

    //    d.coalesce(1).write.csv(fileName+".predict.csv")
  }
}
