package edu.nyu.tandon.experiments.staticpruning.ml.h2o

/**
  * Created by juan on 3/27/17.
  */

import org.apache.spark.sql.SparkSession

object MojoPreProcess {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("\nUse: MojoPreProcess <dataset-parquet>\n")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("MojoPreProcess")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val fileName = args(0)

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

    // H2O model column generic names
    val new_features = new Array[String](features.length)
    for (i <- new_features.indices) {
      new_features(i) = "features" + i.toString
    }

    import org.apache.spark.sql.types.DoubleType

    val df = spark.read.parquet(fileName)
      .select("termID", (Array("docID") ++ features): _*)
      .toDF(Array("termID", "docID") ++ new_features: _*)

    val d = df.select( df.columns.map {
      case termID @ "termID" => df(termID).as(termID)
      case docID @ "docID" => df(docID).as(docID)
      case other => df(other).cast(DoubleType).as(other)
    }:_* )

    d.write.parquet(fileName+".h2o")

  }
}
