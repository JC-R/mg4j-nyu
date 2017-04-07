package edu.nyu.tandon.experiments.staticpruning.ml.features

import java.nio.file.{Files, Paths}

import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by juan on 12/9/16.
  */

object GenerateFeatures {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: GenTrainFiles <labelsFile> <featuresFile> <savePrefix> <samplingSize>")
      System.exit(1)
    }

    val labelsFile = args(0)
    val featuresFile = args(1)
    val saveFiles = args(2)
    val sampleSize = args(3).toDouble

    val spark = SparkSession
      .builder()
      .appName("GenTrainFiles")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // labels from the 100K training queries
    val labels_raw = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").load(labelsFile)
      .withColumn("l_dID", $"_c1".cast("Int"))
      .withColumn("l_tID", $"_c2".cast("Int"))
      .withColumn("rank", $"_c3".cast("Int"))
      .select("l_dID", "l_tID", "rank")

    // read in features file; cleanup extra columns
    val df = spark.read.format("org.apache.spark.sql.execution.datasources.parquet.DefaultSource").load(featuresFile)
      .sample(false, sampleSize)
      .drop("l10_termID", "l10_docID", "l10_top10", "l10_top1K", "l1k_termID", "l1k_docID", "l1k_top10", "l1k_top1K",
        "d_docID", "dh_docID", "ph_docID", "ph_termID")

    // read in training raw labels made with edu.nyu.tandon.experiments.RawHits
    if (!Files.exists(Paths.get(saveFiles+".top10.parquet"))) {

      System.out.println("Generating: " + saveFiles+".top10.parquet");

      // gen training labels, 0 based rank
      val top10 = labels_raw
        .filter($"rank" < 10)
        .groupBy($"l_dID", $"l_tID").count
        .toDF("l_dID", "l_tID", "label")

      df.join(top10, $"p_termID" <=> $"l_tID" && $"p_docID" <=> $"l_dID", "leftouter")
        .drop("l_tID", "l_dID")
        .na.fill(0)
        .write.parquet(saveFiles+".top10.parquet")

    }

    if (!Files.exists(Paths.get(saveFiles+".top1k.parquet"))) {

      System.out.println("Generating: " + saveFiles+".top1k.parquet");

      // gen training labels, 0 based rank
      val top1k = labels_raw.filter($"rank" < 1000)
        .groupBy($"l_dID", $"l_tID")
        .count
        .toDF("l_dID", "l_tID", "label")

      df.join(top1k, $"p_termID" <=> $"l_tID" && $"p_docID" <=> $"l_dID", "leftouter")
        .drop("l_tID", "l_dID")
        .na.fill(0)
        .write.parquet(saveFiles+".top1k.parquet")
    }

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

      inputFiles.foreach{ case (inFile) =>

        if (!Files.exists(Paths.get(inFile + key + ".train.libsvm")) && !Files.exists(Paths.get(inFile + key + ".test.libsvm")) ) {
          System.out.println("Generating: " + key);
          val d1 = spark.read.parquet(inFile + "parquet")
          val d2 = stage.transform(d1).select("label","features")
          val d3 = MLUtils.convertVectorColumnsFromML(d2, "features")
          val d4 = d3.map(row => LabeledPoint(row.getAs[Long]("label"), row.getAs[SparseVector]("features")))
          val splits = d4.randomSplit(Array(0.75,0.25),12345)
          MLUtils.saveAsLibSVMFile(splits(0).rdd.repartition(1), inFile + key + ".train.libsvm")
          MLUtils.saveAsLibSVMFile(splits(1).rdd.repartition(1), inFile + key + ".test.libsvm")
        }
        else
        {
          System.out.println("Skipping: " + key);
        }
      }
    }

    // to get the vector fields:
    // df.schema
    //  .last
    // .metadata
    // .getMetadata("ml_attr")
    // .getMetadata("attrs")
    // .getMetadataArray("numeric")
    // .foreach{s=>println(s.getString("name")


  }
}
