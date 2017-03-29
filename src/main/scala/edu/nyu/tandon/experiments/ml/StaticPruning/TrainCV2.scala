package edu.nyu.tandon.experiments.ml.StaticPruning

import org.apache.spark.ml.feature._
import org.apache.spark.sql._
import ml.dmlc.xgboost4j.scala.spark._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by juan on 12/28/16.
  */
object TrainCV2 {

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      System.err.println("Usage: Train <features> <top10> <top1k> <prefix> <nrounds> <maxDepth> <nthread>")
      System.exit(1)
    }

    val raw_data = args(0)
    val labels_10 = args(1)
    val labels_1k = args(2)
    val prefix = args(3)

    val nrounds = args(4).toInt
    val maxDepth = args(5).toInt
    val nthreads = args(6).toInt

    val spark = SparkSession
      .builder()
      .appName("Train")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    implicit val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // feature groups
    //---------------
    val docFeatures = Array("d_docSize", "d_docTerms", "d_xdoc", "d_doc_s_t", "d_xdoc_s", "d_xdoc_t")

    val termFeatures = Array("p_tfreq", "p_tdfreq", "p_bm25","pt_pt")

    val dhFeatures = Array("dh_b1", "dh_b2", "dh_b3", "dh_b4", "dh_b5", "dh_b6", "dh_b7", "dh_b8", "dh_b9", "dh_b10",
      "dh_b20", "dh_b40", "dh_b80", "dh_b160", "dh_b320", "dh_b640", "dh_b1280",
      "dh_top10", "dh_top1K", "dh_ds_top10", "dh_ds_top1K", "dh_dt_top10", "dh_dt_top1K")

    val phFeatures = Array("ph_b1", "ph_b2", "ph_b3", "ph_b4", "ph_b5", "ph_b6", "ph_b7", "ph_b8", "ph_b9", "ph_b10",
      "ph_b20", "ph_b40", "ph_b80", "ph_b160", "ph_b320", "ph_b640", "ph_b1280",
      "ph_top10", "ph_top1K", "ph_tf_top10", "ph_tf_top1K", "ph_tdf_top10", "ph_tdf_top1K")

    val raw_features = termFeatures ++ docFeatures ++ dhFeatures ++ phFeatures
    val intrinsic_features = docFeatures ++ termFeatures
    val docbased_features = docFeatures ++ dhFeatures
    val termbased_features = termFeatures ++ phFeatures

    val featureGroups = Map(
      "all" -> raw_features,
      "doc" -> docFeatures,
      "term" -> termFeatures,
      "ph" -> phFeatures,
      "dh" -> dhFeatures,
      "termbased" -> termbased_features,
      "docbased" -> docbased_features,
      "intrinsic" -> intrinsic_features
    )

    val paramMap = List(
      "eta" -> 0.023f,
      "max_depth" -> 10,
      "min_child_weight" -> 4.0,
      "subsample" -> 1.0,
      "colsample_bytree" -> 0.82,
      "colsample_bylevel" -> 0.9,
      "seed" -> 49,
      "silent" -> 1,
      "objective" -> "reg:linear").toMap

    val t10 = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(labels_10)
      .na.fill(0)
    println(s"top10 sample: ${t10.count}")

    val t1k = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(labels_1k)
      .na.fill(0)
    println(s"top1k sample: ${t1k.count}")

    featureGroups.foreach { case (key, value) =>

      println("Generating: " + key);

      println("VectorAssembler")
      val stage = new VectorAssembler()
        .setInputCols(value)
        .setOutputCol("features")

      val splits10 = stage.transform(t10)
      val splits1k = stage.transform(t1k)

      val d3 = MLUtils.convertVectorColumnsFromML(splits10, "features")
      val d4 = d3.map(row => LabeledPoint(row.getAs[Long]("top10"), row.getAs[SparseVector]("features")))
      val splits = d4.randomSplit(Array(0.75,0.25),12345)
      MLUtils.saveAsLibSVMFile(splits(0).rdd.repartition(1), prefix + ".top10." + key + ".train.libsvm")
      MLUtils.saveAsLibSVMFile(splits(1).rdd.repartition(1), prefix + ".top10." + key + ".test.libsvm")

      val d5 = MLUtils.convertVectorColumnsFromML(splits1k, "features")
      val d6 = d5.map(row => LabeledPoint(row.getAs[Long]("top1K"), row.getAs[SparseVector]("features")))
      val splits2 = d6.randomSplit(Array(0.75,0.25),12345)
      MLUtils.saveAsLibSVMFile(splits2(0).rdd.repartition(1), prefix + ".top1k." + key + ".train.libsvm")
      MLUtils.saveAsLibSVMFile(splits2(1).rdd.repartition(1), prefix + ".top1k." + key + ".test.libsvm")

      //      println("Xgboost training top1k")
//      val xg1k = XGBoost.trainWithDataFrame(splits1k(0), paramMap, round = nrounds, labelCol = "top1k", nWorkers = nthreads, useExternalMemory = true)
//      xg1k.saveModelAsHadoopFile(prefix + "." + key + ".top1k.model")

//      // DataFrames can be saved as Parquet files, maintaining the schema information
//      val predictions1k = xg1k.setExternalMemory(true).transform(splits1k(1)).select("top1k", "prediction")
//      predictions1k.write.save("pred.test.top1k." + key +".parquet")

    }
    spark.stop()
  }

}