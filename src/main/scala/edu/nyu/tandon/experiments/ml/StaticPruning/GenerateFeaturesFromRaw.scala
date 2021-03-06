package edu.nyu.tandon.experiments.ml.StaticPruning

import java.nio.file.{Files, Paths}

import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import scala.collection.mutable._

/**
  * Created by juan on 12/9/16.
  */

object GenerateFeaturesFromRaw {


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

    val ph = spark.read.option("header",false).csv(phFn)
      .toDF("ph_docID","ph_termID","ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
        "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280","u1","u2","u3","u4","u5")
      .withColumn("ph_termID", $"ph_termID".cast("Int"))
      .withColumn("ph_docID", $"ph_docID".cast("Int"))
      .withColumn("ph_b1", $"ph_b1".cast("Int"))
      .withColumn("ph_b2", $"ph_b2".cast("Int"))
      .withColumn("ph_b3", $"ph_b3".cast("Int"))
      .withColumn("ph_b4", $"ph_b4".cast("Int"))
      .withColumn("ph_b5", $"ph_b5".cast("Int"))
      .withColumn("ph_b6", $"ph_b6".cast("Int"))
      .withColumn("ph_b7", $"ph_b7".cast("Int"))
      .withColumn("ph_b8", $"ph_b8".cast("Int"))
      .withColumn("ph_b9", $"ph_b9".cast("Int"))
      .withColumn("ph_b10", $"ph_b10".cast("Int"))
      .withColumn("ph_b20", $"ph_b20".cast("Int"))
      .withColumn("ph_b40", $"ph_b40".cast("Int"))
      .withColumn("ph_b80", $"ph_b80".cast("Int"))
      .withColumn("ph_b160", $"ph_b160".cast("Int"))
      .withColumn("ph_b320", $"ph_b320".cast("Int"))
      .withColumn("ph_b640", $"ph_b640".cast("Int"))
      .withColumn("ph_b1280", $"ph_b1280".cast("Int"))
      .withColumn("ph_top10", $"ph_b1"+$"ph_b2"+$"ph_b3"+$"ph_b4"+$"ph_b5"+$"ph_b6"+$"ph_b7"+$"ph_b8"+$"ph_b9"+$"ph_b10")
      .withColumn("ph_top1K", $"ph_top10"+$"ph_b20"+$"ph_b40"+$"ph_b80"+$"ph_b160"+$"ph_b320"+$"ph_b640"+$"ph_b1280")
      .drop("u1","u2","u3","u4","u5")

    ph.show()

    val dh = spark.read.option("header",false).option("delimiter"," ").csv(dhFn)
      .withColumn("dh_docID", $"_c0".cast("Int"))
      .withColumn("dh_b1", $"_c1".cast("Int"))
      .withColumn("dh_b2", $"_c2".cast("Int"))
      .withColumn("dh_b3", $"_c3".cast("Int"))
      .withColumn("dh_b4", $"_c4".cast("Int"))
      .withColumn("dh_b5", $"_c5".cast("Int"))
      .withColumn("dh_b6", $"_c6".cast("Int"))
      .withColumn("dh_b7", $"_c7".cast("Int"))
      .withColumn("dh_b8", $"_c8".cast("Int"))
      .withColumn("dh_b9", $"_c9".cast("Int"))
      .withColumn("dh_b10", $"_c10".cast("Int"))
      .withColumn("dh_b20", $"_c11".cast("Int"))
      .withColumn("dh_b40", $"_c12".cast("Int"))
      .withColumn("dh_b80", $"_c13".cast("Int"))
      .withColumn("dh_b160", $"_c14".cast("Int"))
      .withColumn("dh_b320", $"_c15".cast("Int"))
      .withColumn("dh_b640", $"_c16".cast("Int"))
      .withColumn("dh_b1280", $"_c17".cast("Int"))
      .withColumn("dh_top10", $"dh_b1"+$"dh_b2"+$"dh_b3"+$"dh_b4"+$"dh_b5"+$"dh_b6"+$"dh_b7"+$"dh_b8"+$"dh_b9"+$"dh_b10")
      .withColumn("dh_top1K", $"dh_top10"+$"dh_b20"+$"dh_b40"+$"dh_b80"+$"dh_b160"+$"dh_b320"+$"dh_b640"+$"dh_b1280")
      .select("dh_docID","dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
        "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280","dh_top10","dh_top1K")

    dh.show()

    // posting features
    val p = spark.read.option("header",false).csv(pFn).toDF("p_termID","p_docID","p_tfreq","p_tdfreq","p_bm25")
        .withColumn("p_termID", $"p_termID".cast("Int"))
        .withColumn("p_docID", $"p_docID".cast("Int"))
        .withColumn("p_tfreq", $"p_tfreq".cast("Int"))
        .withColumn("p_tdfreq", $"p_tdfreq".cast("Int"))
        .withColumn("p_bm25", $"p_bm25".cast("Double"))

    p.show()
    // doc features
    val d = spark.read.option("header",false).csv(dhFn).toDF("d_docID","d_docSize","d_docTerms","d_xdoc")
      .withColumn("d_docID", $"d_docID".cast("Int"))
      .withColumn("d_docSize",$"d_docSize".cast("Int"))
      .withColumn("d_docTerms",$"d_docTerms".cast("Int"))
      .withColumn("d_xdoc",$"d_xdoc".cast("Double"))
      .withColumn("d_doc_s_t",$"d_docSize".cast("Double")/$"d_docTerms")
      .withColumn("d_xdoc_s",$"d_xdoc"/$"d_docSize")
      .withColumn("d_xdoc_t",$"d_xdoc"/$"d_docTerms")

    d.show()

    // join
    val f1 = p.join(d, p("p_docID") === d("d_docID"),"inner")

    val f2 = f1.join(dh, p("p_docID") === dh("dh_docID"), "left_outer")
      .withColumn("dh_ds_top10",$"dh_top10".cast("Double")/$"d_docSize")
      .withColumn("dh_ds_top1K",$"dh_top1K".cast("Double")/$"d_docSize")
      .withColumn("dh_dt_top10",$"dh_top10".cast("Double")/$"d_docTerms")
      .withColumn("dh_dt_top1K",$"dh_top1K".cast("Double")/$"d_docTerms")

    val f3 = f2.join(ph, p("p_docID") === ph("ph_docID") && p("p_termID") === ph("ph_termID"), "left_outer")
      .withColumn("ph_tf_top10",$"ph_top10".cast("Double")/$"p_tfreq")
      .withColumn("ph_tf_top1K",$"ph_top1K".cast("Double")/$"p_tfreq")
      .withColumn("ph_tdf_top10",$"ph_top10".cast("Double")/$"p_tdfreq")
      .withColumn("ph_tdf_top1K",$"ph_top1K".cast("Double")/$"p_tdfreq")
      .na.fill(0)

    f3.show
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
