package edu.nyu.tandon.experiments.staticpruning.ml.features

import org.apache.spark.sql.SparkSession

import scala.collection.mutable._

/**
  * Created by juan on 12/9/16.
  */

object GenerateFeaturesFromRaw3 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GenFeatures")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // join
    val p = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("cw09b/p.parquet")
      .withColumnRenamed("p_termID","termID")
      .withColumnRenamed("p_docID","docID")

    val pt = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("cw09b/pt.parquet")
      .withColumnRenamed("pt_termID","termID")

    val d = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("cw09b/d.parquet")
      .withColumnRenamed("d_docID","docID")

    val dh = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("cw09b/dh.parquet")
      .withColumnRenamed("dh_docID","docID")

    val ph = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("cw09b/ph.parquet")
      .withColumnRenamed("ph_termID","termID")
      .withColumnRenamed("ph_docID","docID")

    // generate training sets
    val top10 = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("cw09b/cw09b.100K.OR.top10.parquet")
      .toDF("termID","docID","label_top10")

    val top1k = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load("cw09b/cw09b.100K.OR.top1k.parquet")
      .toDF("termID","docID","label_top1k")

    val d1 = p.join(pt, Seq("termID"),"left_outer")
//    d1.write.parquet("p1.parquet")

//    val p1 = spark.read.parquet("p1.parquet")
//      .repartition(spark.sparkContext.defaultParallelism * 3)
    val d2 = d1.join(d, Seq("docID"), "left_outer")
//    d2.write.parquet("p2.parquet")

//    val p2 = spark.read.parquet("p2.parquet")
//      .repartition(spark.sparkContext.defaultParallelism * 3)
    val d3 = d2.join(dh, Seq("docID"), "left_outer")
//    d3.write.parquet("p3.parquet")

//    val p3 = spark.read.parquet("p3.parquet")
    val d4 = d3.join(ph, Seq("docID","termID"), "left_outer")
//  d4.write.parquet("p4.parquet")

  //  val p4 = spark.read.parquet("p4.parquet")
    val d5 = d4
      .withColumn("ph_top10_d", $"ph_top10".cast("Double"))
      .withColumn("ph_top1K_d",$"ph_top1K".cast("Double"))
      .withColumn("dh_top10_d",$"dh_top10".cast("Double"))
      .withColumn("dh_top1k_d",$"dh_top1K".cast("Double"))
      .withColumn("ph_tf_top10",$"ph_top10_d"/$"p_tfreq")
      .withColumn("ph_tf_top1K",$"ph_top1k_d"/$"p_tfreq")
      .withColumn("ph_tdf_top10",$"ph_top10_d"/$"p_tdfreq")
      .withColumn("ph_tdf_top1K",$"ph_top1k_d"/$"p_tdfreq")
      .withColumn("dh_ds_top10",$"dh_top10_d"/$"d_docSize")
      .withColumn("dh_ds_top1K",$"dh_top1k_d"/$"d_docSize")
      .withColumn("dh_dt_top10",$"dh_top10_d"/$"d_docTerms")
      .withColumn("dh_dt_top1K",$"dh_top1k_d"/$"d_docTerms")
      .drop("ph_top10_d","ph_top1k_d","dh_top10_d","dh_top1k_d")
//    d5.write.parquet("p5.parquet")

//    val p5 = spark.read.parquet("p5.parquet")
    val d6 = d5.join(top10, Seq("docID","termID"), "left_outer")
    val d7 = d6.join(top1k, Seq("docID","termID"), "left_outer")

    //    f3.show
    d7.write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .save("cw09b.raw_features.parquet")
  }
}
