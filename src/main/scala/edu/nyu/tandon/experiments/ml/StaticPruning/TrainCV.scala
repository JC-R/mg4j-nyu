package edu.nyu.tandon.experiments.ml.StaticPruning

import org.apache.spark.sql._
import scala.collection.mutable
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{SparseVector,DenseVector}
import scala.collection.mutable
import org.apache.spark.sql.functions.lit
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.ml.feature._
import org.apache.spark.sql._

/**
  * Created by juan on 12/28/16.
  */
object TrainCV {

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

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val d_raw = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(raw_data)
      .withColumnRenamed("label_top10","top10")
      .withColumnRenamed("label_top1K","top1K")
    val tot = d_raw.count
    println(s"postings: $tot")

    // generate training sets
    val top10_count = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(labels_10)
      .count()
    println(s"top10 labels: $top10_count")

    val top1k_count = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(labels_1k)
      .count()
    println(s"top1k labels: $top1k_count")

    // get a balanced sample
    println("generating top10 sample")
    val d1 = d_raw.filter($"top10" > 0)
    val d2 = d_raw.filter($"top10".isNull || $"top10" === 0).sample(false,1.0 * top10_count / tot )
    val d3 = d1.union(d2)
      d3.write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .save(prefix + ".train.top10.parquet")

    println("generating top1k sample")
    val d4 = d_raw.filter($"top1k" > 0)
    val d5 = d_raw.filter($"top1k".isNull || $"top1k" === 0).sample(false,1.0 * top1k_count / tot )
    val d6 = d4.union(d5)
      d6.write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .save(prefix + ".train.top1k.parquet")

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
      "intrinsic" -> intrinsic_features,
      "docbased" -> docbased_features
    )

    val paramMap = List(
      "eta" -> 0.023f,
      "max_depth" -> 10,
      "min_child_weight" -> 3.0,
      "subsample" -> 1.0,
      "colsample_bytree" -> 0.82,
      "colsample_bylevel" -> 0.9,
      "seed" -> 49,
      "silent" -> 0,
      "objective" -> "reg:linear").toMap

    val t10 = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(prefix + ".train.top10.parquet")
    t10.persist()
    println(s"top10 sample: ${t10.count}")

    val t1k = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(prefix + ".train.top10.parquet")
    t1k.persist()
    println(s"top1k sample: ${t1k.count}")

    featureGroups.foreach { case (key, value) =>

      println("Generating: " + key);

      println("VectorAssembler")
      val stage = new VectorAssembler()
        .setInputCols(value)
        .setOutputCol("features")

      val splits10 = stage.transform(t10).select("top10","features").randomSplit(Array(0.7, 0.3), 123L)
      val splits1k = stage.transform(t1k).select("top1k","features").randomSplit(Array(0.7, 0.3), 123L)

      implicit val sc1 = sc

      println("Xgboost training top10")
      val xg10 = ml.dmlc.xgboost4j.scala.spark.XGBoost.trainWithDataFrame(splits10(0), paramMap, round = nrounds,labelCol = "top10", nWorkers = nthreads, useExternalMemory = true)
      xg10.saveModelAsHadoopFile(prefix + "." + key + ".top10.model")

      // DataFrames can be saved as Parquet files, maintaining the schema information
      val predictions10 = xg10.setExternalMemory(true).transform(splits10(1)).select("top10", "prediction")
      predictions10.write.save("preds_top10_" + key +".parquet")

      println("Xgboost training top1k")
      val xg1k = ml.dmlc.xgboost4j.scala.spark.XGBoost.trainWithDataFrame(splits1k(0), paramMap, round = nrounds, labelCol = "top1k", nWorkers = nthreads, useExternalMemory = true)
      xg1k.saveModelAsHadoopFile(prefix + "." + key + ".top1k.model")

      // DataFrames can be saved as Parquet files, maintaining the schema information
      val predictions1k = xg10.setExternalMemory(true).transform(splits10(1)).select("top10", "prediction")
      predictions1k.write.save("preds.top1k." + key +".parquet")

    }
    spark.stop()
  }

}