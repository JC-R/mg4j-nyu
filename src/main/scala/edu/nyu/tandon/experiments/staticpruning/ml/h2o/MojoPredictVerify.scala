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
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MojoPredictVerify {

  def toDouble: (Any) => Double = {
    case null => Double.NaN
    case i: Int => i
    case f: Float => f
    case d: Double => d
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("\nUse: MojoPredict <model> <dataset> <prediction>\n")
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

    //    val modelName = "/mnt/scratch/gbm_top1k_all.zip"
    //    val fileName = "/mnt/scratch/cw09b/cw09b.features.parquet"


    // load the H2O model
    val m = MojoModel.load(modelName)
    val model = new EasyPredictModelWrapper(m)
    val modelBroadcast = spark.sparkContext.broadcast(model)

    val modelFeatures = m.getNames.slice(0, m.nfeatures())
    val features = spark.sparkContext.broadcast(modelFeatures)

    val df = spark.read
      .option("header", "true")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .csv(fileName)

    if ((modelFeatures diff df.columns).length != 0) {
      System.err.println("\n***ERROR: model and data field mismatch.\n")
      System.exit(1)
    }

    val d = df.map(r => {

      var row = new RowData

      features.value.foreach(f => {
        val col = r.getString(r.fieldIndex(f))
        if (col == null)
          row.put(f, new java.lang.Double(Double.NaN))
        else
          row.put(f, new java.lang.Double(col))
      })

      val prediction = modelBroadcast.value.predictRegression(row).value
      row.clear()
      row = null
      (r.getString(r.fieldIndex("termID")), r.getString(r.fieldIndex("docID")), prediction)

    }).toDF("termID", "docID", "pred")

    val d2 = d.join(df, Seq("termID", "docID"), "inner").orderBy(desc("pred"))

    d2.write
      .option("header", "true")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .csv(args(2))

    //    d.coalesce(1).write.csv(fileName+".predict.csv")
  }
}
