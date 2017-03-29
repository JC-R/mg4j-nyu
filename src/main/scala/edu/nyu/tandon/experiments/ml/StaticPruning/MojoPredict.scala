package edu.nyu.tandon.experiments.ml.StaticPruning

/**
  * Created by juan on 3/27/17.
  */

import org.apache.spark._
import java.io._

import _root_.hex.genmodel.easy.RowData
import _root_.hex.genmodel.easy.EasyPredictModelWrapper
import _root_.hex.genmodel.easy.prediction._
import _root_.hex.genmodel.MojoModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.DoubleType

object MojoPredict {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("\nUse: MojoPredict <model> <dataset-parquet>\n")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("MojoPredict")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //    val modelName = "/mnt/scratch/gbm_top1k_all.zip"
    //    val fileName = "/mnt/scratch/cw09b/cw09b.features.parquet"

    val modelName = args(0)
    val fileName = args(1)

    import spark.implicits._

    // feature groups
    //---------------
    val features = Array(
      "p_tfreq", "p_tdfreq", "p_bm25", "pt_pt",
      "d_docSize", "d_docTerms", "d_xdoc", "d_doc_s_t", "d_xdoc_s", "d_xdoc_t",
      "dh_b1", "dh_b2", "dh_b3", "dh_b4", "dh_b5", "dh_b6", "dh_b7", "dh_b8", "dh_b9", "dh_b10",
      "dh_b20", "dh_b40", "dh_b80", "dh_b160", "dh_b320", "dh_b640", "dh_b1280",
      "dh_top10", "dh_top1K", "dh_ds_top10", "dh_ds_top1K", "dh_dt_top10", "dh_dt_top1K",
      "ph_b1", "ph_b2", "ph_b3", "ph_b4", "ph_b5", "ph_b6", "ph_b7", "ph_b8", "ph_b9", "ph_b10",
      "ph_b20", "ph_b40", "ph_b80", "ph_b160", "ph_b320", "ph_b640", "ph_b1280", "ph_top10", "ph_top1K",
      "ph_tf_top10", "ph_tf_top1K", "ph_tdf_top10", "ph_tdf_top1K"
    )

    // part 2 - score/predict
    // load the H2O model (Regression only)
    val model = new EasyPredictModelWrapper(MojoModel.load(modelName))

    val df = spark.read.parquet(fileName)

    val d = df.map(r => {
      val row = new RowData
      for (i <- 0 to features.length-3) {
        row.put("features"+i.toString, new java.lang.Double(r.getDouble(2+i)))
      }
      (r.getInt(0), r.getInt(1), model.predictRegression(row).value)
    }).toDF("termID", "docID", "predict")

    import org.apache.spark.sql.functions._

    d.orderBy(desc("predict")).write.parquet(fileName + ".predict")

  }
}
