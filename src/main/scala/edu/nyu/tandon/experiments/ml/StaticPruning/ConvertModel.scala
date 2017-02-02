package edu.nyu.tandon.experiments.ml.StaticPruning

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import biz.k11i.xgboost.Predictor
import biz.k11i.xgboost.util.FVec
import ml.dmlc.xgboost4j.scala.spark._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/**
  * Created by juan on 1/1/17.
  */
object ConvertModel {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("\nUse: Predict <model> <outModel>\n")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("StaticPruning ML - Extreact XGBoost Model")
      .getOrCreate()

    spark.conf.set("spark.serializer","org.apache.serializer.KyroSerializer")
    spark.conf.set("spark.kyroserializer.buffer.max","2g")

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    implicit val sc = spark.sparkContext

    val modelName = args(0)

    println(args(0))

      // create java- native Predictor
    val booster = XGBoost.loadModelFromHadoopFile(modelName)
    val os = new ByteArrayOutputStream()
    booster.booster.saveModel(args(1))

    spark.stop()
  }
}
