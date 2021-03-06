package edu.nyu.tandon.experiments.ml.StaticPruning

import org.apache.spark.ml.feature._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import ml.dmlc.xgboost4j.scala.spark._
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.ml.linalg.{SparseVector,DenseVector}

import scala.collection.mutable

/**
  * Created by juan on 1/1/17.
  */
object Predict {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: Predict <model> <dataset> <outfile>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("MLPredict")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // feature groups
    //---------------
    val raw_features = Array(
      "p_tfreq", "p_tdfreq", "p_bm25",
      "d_docSize", "d_docTerms", "d_xdoc", "d_doc_s_t", "d_xdoc_s", "d_xdoc_t",
      "dh_b1", "dh_b2", "dh_b3", "dh_b4", "dh_b5", "dh_b6", "dh_b7", "dh_b8", "dh_b9", "dh_b10",
      "dh_b20", "dh_b40", "dh_b80", "dh_b160", "dh_b320", "dh_b640", "dh_b1280",
      "dh_top10", "dh_top1K", "dh_ds_top10", "dh_ds_top1K", "dh_dt_top10", "dh_dt_top1K",
      "ph_b1", "ph_b2", "ph_b3", "ph_b4", "ph_b5", "ph_b6", "ph_b7", "ph_b8", "ph_b9", "ph_b10",
      "ph_b20", "ph_b40", "ph_b80", "ph_b160", "ph_b320", "ph_b640", "ph_b1280", "ph_top10", "ph_top1K",
      "ph_tf_top10", "ph_tf_top1K", "ph_tdf_top10", "ph_tdf_top1K"
    )

    val docFeatures = Array("d_docSize", "d_docTerms", "d_xdoc", "d_doc_s_t", "d_xdoc_s", "d_xdoc_t")

    val termFeatures = Array("p_tfreq", "p_tdfreq", "p_bm25")

    val dhFeatures = Array("dh_b1", "dh_b2", "dh_b3", "dh_b4", "dh_b5", "dh_b6", "dh_b7", "dh_b8", "dh_b9", "dh_b10",
      "dh_b20", "dh_b40", "dh_b80", "dh_b160", "dh_b320", "dh_b640", "dh_b1280",
      "dh_top10", "dh_top1K", "dh_ds_top10", "dh_ds_top1K", "dh_dt_top10", "dh_dt_top1K")

    val phFeatures = Array("ph_b1", "ph_b2", "ph_b3", "ph_b4", "ph_b5", "ph_b6", "ph_b7", "ph_b8", "ph_b9", "ph_b10",
      "ph_b20", "ph_b40", "ph_b80", "ph_b160", "ph_b320", "ph_b640", "ph_b1280",
      "ph_top10", "ph_top1K", "ph_tf_top10", "ph_tf_top1K", "ph_tdf_top10", "ph_tdf_top1K")

    val intrinsic_features = docFeatures ++ termFeatures
    val docbased_features = docFeatures ++ dhFeatures
    val termbased_features = termFeatures ++ phFeatures

    val featureGroups = Map(
      "doc" -> docFeatures,
      "term" -> termFeatures,
      "dh" -> dhFeatures,
      "ph" -> phFeatures,
      "intrinsic" -> intrinsic_features,
      "docbased" -> docbased_features,
      "termbased" -> termbased_features,
      "all" -> raw_features
    )

    val df = spark.read.parquet(args(1))
    val booster = ml.dmlc.xgboost4j.scala.XGBoost.loadModel(args(0))
    val regressor = new ml.dmlc.xgboost4j.scala.spark.XGBoostRegressionModel(booster)

    featureGroups.foreach { case (key, value) =>

      val stage = new VectorAssembler()
        .setInputCols(value)
        .setOutputCol("features")

      val d2 = stage.transform(df)
      val d3: RDD[DenseVector] = d2.rdd.map(row => row.getAs[SparseVector]("features").toDense)
      val result = regressor.predict(d3,0)
      result.saveAsTextFile(args(2)+"." + key + ".predict")
    }
  }
}
