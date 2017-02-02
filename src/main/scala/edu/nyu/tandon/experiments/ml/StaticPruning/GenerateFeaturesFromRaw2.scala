package edu.nyu.tandon.experiments.ml.StaticPruning

import java.nio.file.{Files, Paths}

import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import scala.collection.mutable._

import java.nio.file.{Paths, Files}

/**
  * Created by juan on 12/9/16.
  */

object GenerateFeaturesFromRaw2 {


  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      System.err.println("Usage: GenTrainFiles <labelsFile> <postingsFeatures> <docFeatures> <dhFeatures> <phFeatures> <savePrefix> <samplingSize>")
      System.exit(1)
    }

    val labelsFile = args(0)
    val pFn = args(1)
    val dFn = args(2)
    val dhFn = args(3)
    val phFn = args(4)
    val saveFiles = args(5)
    val sampleSize = args(6).toDouble

    val spark = SparkSession
      .builder()
      .appName("GenFeatures")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // posting features
    val p = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header",false)
      .load(pFn)
      .repartition(spark.sparkContext.defaultParallelism * 3)
      .toDF("p_termID","p_docID","p_tfreq","p_tdfreq","p_bm25")
      .withColumn("p_termID", $"p_termID".cast("Int"))
      .withColumn("p_docID", $"p_docID".cast("Int"))
      .withColumn("p_tfreq", $"p_tfreq".cast("Int"))
      .withColumn("p_tdfreq", $"p_tdfreq".cast("Int"))
      .withColumn("p_bm25", $"p_bm25".cast("Double"))

    p.write.parquet("cw09b/p.parquet")
//    p.show()

    // join

    val p1 = spark.read.parquet("cw09b/p.parquet")
    val d1 = spark.read.parquet("cw09b/d.parquet")
    val f1 = p1.join(d1, p("p_docID") === d1("d_docID"),"inner")

    val dh1 = spark.read.parquet("cw09b/dh.parquet")
    val f2 = f1.join(dh1, p("p_docID") === dh1("dh_docID"), "left_outer")
      .withColumn("dh_ds_top10",$"dh_top10".cast("Double")/$"d_docSize")
      .withColumn("dh_ds_top1K",$"dh_top1K".cast("Double")/$"d_docSize")
      .withColumn("dh_dt_top10",$"dh_top10".cast("Double")/$"d_docTerms")
      .withColumn("dh_dt_top1K",$"dh_top1K".cast("Double")/$"d_docTerms")

    val ph1 = spark.read.parquet("cw09b/ph.parquet")
    val f3 = f2.join(ph1, p("p_docID") === ph1("ph_docID") && p("p_termID") === ph1("ph_termID"), "left_outer")
      .withColumn("ph_tf_top10",$"ph_top10".cast("Double")/$"p_tfreq")
      .withColumn("ph_tf_top1K",$"ph_top1K".cast("Double")/$"p_tfreq")
      .withColumn("ph_tdf_top10",$"ph_top10".cast("Double")/$"p_tdfreq")
      .withColumn("ph_tdf_top1K",$"ph_top1K".cast("Double")/$"p_tdfreq")
      .na.fill(0)

//    f3.show
    f3.write.parquet(saveFiles + ".raw_features.parquet")

    // read in features file; cleanup extra columns
    val df = spark.read.parquet(saveFiles + ".raw_features.parquet")

    df.drop("d_docID","dh_docID","ph_docID","ph_termID")
      .sample(false, sampleSize)
      .write.parquet(saveFiles + ".sample.raw_features.parquet")

    val sample = spark.read.parquet(saveFiles + ".sample.raw_features.parquet")

    // read in training raw labels made with edu.nyu.tandon.tool.RawHits
    val raw_features = Array("d_docSize","d_docTerms","d_xdoc","d_doc_s_t","d_xdoc_s","d_xdoc_t",
      "p_tfreq","p_tdfreq","p_bm25",
      "ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
      "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280","ph_top10","ph_top1K",
      "ph_tf_top10","ph_tf_top1K","ph_tdf_top10","ph_tdf_top1K",
      "dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
      "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280",
      "dh_top10","dh_top1K","dh_ds_top10","dh_ds_top1K","dh_dt_top10","dh_dt_top1K"
    )

    val docFeatures = Array("d_docSize","d_docTerms","d_xdoc","d_doc_s_t","d_xdoc_s","d_xdoc_t")

    val termFeatures = Array("p_tfreq","p_tdfreq","p_bm25")

    val dhFeatures = Array("dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
      "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280",
      "dh_top10","dh_top1K","dh_ds_top10","dh_ds_top1K","dh_dt_top10","dh_dt_top1K")

    val phFeatures = Array("ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
      "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280",
      "ph_top10","ph_top1K","ph_tf_top10","ph_tf_top1K","ph_tdf_top10","ph_tdf_top1K")

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

    val inputFiles = Array(saveFiles+".top1k.", saveFiles+".top10.")

    featureGroups.foreach { case (key, value) =>

      val stage = new VectorAssembler()
        .setInputCols(value)
        .setOutputCol("features")

      val d1 = stage.transform(df).select("p_termID","p_docID","features")
      d1.write.parquet(saveFiles + ".features.parquet")

      stage.transform(sample)
        .select("p_termID","p_docID","features")
        .write.parquet(saveFiles + ".sample.features.parquet")

      d1.schema
        .last
        .metadata
        .getMetadata("ml_attr")
        .getMetadata("attrs")
        .getMetadataArray("numeric")
        .foreach{s=>println(s.getString("name"))}

    }
  }
}
