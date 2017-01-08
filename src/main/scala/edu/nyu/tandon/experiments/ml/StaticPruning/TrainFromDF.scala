package edu.nyu.tandon.experiments.ml.StaticPruning

import java.io.File

import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._

import scala.collection.mutable

/**
  * Created by juan on 12/28/16.
  */
object TrainFromDF {

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      System.err.println("Usage: GenPredicts <labelsFile> <featuresFile> <sampleSize> <savePrefix> <rounds> <maxDepth> <nthreads> <nwokrers> <extMem>")
      System.exit(1)
    }

    val labelsFile = args(0)
    val featuresFile = args(1)
    val sampleSize = args(2).toDouble
    val saveFiles = args(3)
    val nRounds = args(4).toInt
    val maxDepth = args(5).toInt
    val nthreads = args(6).toInt
    val nWorkers = args(7).toInt
    val extMem = args(8).toBoolean

    val spark = SparkSession
      .builder()
      .appName("GenTrainFiles")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // read in training raw labels made with edu.nyu.tandon.tool.RawHits
    val labels_raw = spark.read.format("csv").load(labelsFile)
      .withColumn("l_dID", $"_c1".cast("Int"))
      .withColumn("l_tID", $"_c2".cast("Int"))
      .withColumn("rank", $"_c3".cast("Int"))
      .select("l_dID", "l_tID", "rank")

    // gen training labels, 0 based rank
    val top10 = labels_raw
      .filter($"rank" < 10)
      .groupBy($"l_dID", $"l_tID").count
      .toDF("l_dID", "l_tID", "label")

    val top1k = labels_raw.filter($"rank" < 1000)
      .groupBy($"l_dID", $"l_tID")
      .count
      .toDF("l_dID", "l_tID", "label")

    // read in features file; cleanup extra columns
    val df = spark.read.parquet(featuresFile)
      .drop("l10_termID", "l10_docID", "l10_top10", "l10_top1K", "l1k_termID", "l1k_docID", "l1k_top10", "l1k_top1K",
        "d_docID", "dh_docID", "ph_docID", "ph_termID")

    val data = df.sample(false, sampleSize)

    data.join(top10, $"p_termID" <=> $"l_tID" && $"p_docID" <=> $"l_dID", "leftouter")
      .drop("l_tID", "l_dID")
      .na.fill(0)
      .write.parquet(saveFiles+".top10.parquet")

    data.join(top1k, $"p_termID" <=> $"l_tID" && $"p_docID" <=> $"l_dID", "leftouter")
      .drop("l_tID", "l_dID")
      .na.fill(0)
      .write.parquet(saveFiles+".top1k.parquet")

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

    val params = new mutable.HashMap[String, Any]()
    params += "eta" -> 0.02
    params += "alpha" -> 0.1
    params += "objective" -> "reg:linear"
    params += "booster" -> "gbtree"
    params += "min_child_weight" -> 4
    params += "max_depth" -> maxDepth
    params += "silent" -> 0
    params += "tree_method" -> "approx"
    params += "save_period" -> 50
    params += "seed" -> 12345
    params += "nthread" -> nthreads

    val inputFiles = Array(saveFiles+".top1k.", saveFiles+".top10.")

    featureGroups.foreach { case (key, value) =>

      val stage = new VectorAssembler()
        .setInputCols(value)
        .setOutputCol("features")

      // full dataset
      val full = stage.transform(data).select("label","features")

      // training dataset
      inputFiles.foreach{ case (inFile) =>

        val d1 = spark.read.parquet(inFile + "parquet")
        val d2 = stage.transform(d1).select("label","features")
        d2.write.parquet(inFile+"parquet")

        // train model
        val booster = XGBoost.trainWithDataFrame(d2, params.toMap, nRounds, nWorkers = nWorkers, useExternalMemory = extMem)

        // predict on full dataset
        booster.transform(full).write.parquet(inFile + "full.parquet")
      }
    }
  }
}
