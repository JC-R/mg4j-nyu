package edu.nyu.tandon.experiments.staticpruning.ml.h2o

/**
  * H2O MOJO Predictor
  *
  * Params: MOJO model, zipped
  * Dataset w/features, parquet
  *
  * Sample command line:
  * spark-submit <spark args> --packages ai.h2o:h20-genmodel:3.10.4.2 --class edu.nyu.tandon.StaticPruning.H2OMojoPredict mg4j-nyu.jar gbm_top1k_all.zip cw09b.features.parquet
  *
  * Created by juan on 3/27/17.
  */

import _root_.hex.genmodel.MojoModel
import _root_.hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MojoPredict {

  def toDouble: (Any) => Double = {
    case null => Double.NaN
    case i: Int => i
    case f: Float => f
    case d: Double => d
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("\nUse: MojoPredict <model> <dataset-parquet> <prediction>\n")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("MojoPredict")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    //    val modelName = "/mnt/scratch/gbm_top1k_all.zip"
    //    val fileName = "/mnt/scratch/cw09b/cw09b.features.parquet"

    val modelName = args(0)
    val fileName = args(1)

    // load the H2O model
    val m = MojoModel.load(modelName)
    val model = new EasyPredictModelWrapper(m)

    val modelFeatures = m.getNames().filterNot(e => e == m.getResponseName())

    val df = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .parquet(fileName)

    // verify model features are included in the data
    if ((modelFeatures diff df.columns).length != 0) {
      System.err.println("\n***ERROR: model and data field mismatch.\n")
      System.exit(1)
    }

    val d = df.map(r => {
      val row = new RowData
      modelFeatures.foreach(f => row.put(f, new java.lang.Double(toDouble(r.get(r.fieldIndex(f))))))
      (r.getInt(r.fieldIndex("termID")), r.getInt(r.fieldIndex("docID")), model.predictRegression(row).value)
    }).toDF("termID", "docID", "predict")


    d.orderBy(desc("predict"))
      .write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .parquet(args(2))

    //    d.coalesce(1).write.csv(fileName+".predict.csv")
  }
}
