package edu.nyu.tandon.experiments.staticpruning.ml.xgboost

import java.io.FileInputStream

import biz.k11i.xgboost.Predictor
import biz.k11i.xgboost.util.FVec
import org.apache.spark.ml.linalg._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.desc

/**
  * Created by juan on 1/1/17.
  */
object PredictAll {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("\nUse: Predict <model> <outDir>\n")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("StaticPruning ML Predict " + args(0))
      .getOrCreate()

    spark.conf.set("spark.serializer","org.apache.serializer.KyroSerializer")
    spark.conf.set("spark.kyroserializer.buffer.max","2g")

    //    if (spark.conf.get("spark.driver.extraLibraryPath").length()>0 &&
    //    spark.conf.get("spark.executor.extraLibraryPath").length==0)
    //      spark.conf.set("spark.executor.extraLibraryPath",spark.conf.get("spark.driver.extraLibraryPath"))

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val d = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(args(1) + ".vectors")

    implicit val sc = spark.sparkContext
    implicit val encoder = RowEncoder(d.schema)

    val modelName = args(0) + "." + args(1)

    Array("top1k","top10").foreach( label => {

      println(args(1) + " " + label)

      // create java- native Predictor
      val predictor = new Predictor(new FileInputStream(modelName + "." + label + ".model.xg"))

      // send predictor object to all the workers/nodes
      val broadcastBooster = spark.sparkContext.broadcast(predictor)

      //      val mapped = d.mapPartitions ( partition => {
      //        partition.map(row => {
      //          val (term, doc, featV) = (row.getAs[Int]("termID"), row.getAs[Int]("docID"), row.getAs[Vector]("features"))
      //          val pred = predictor.predictSingle(FVec.Transformer.fromArray(featV.toDense.toArray, false))
      //          (term, doc, pred)
      //        }).toIterator
      //      }).toDF("termID","docID","prediciton")
      //
      val mapped = d.map( row => {
        val (term, doc, featV) = (row.getAs[Int]("termID"), row.getAs[Int]("docID"), row.getAs[Vector]("features"))
        val pred = predictor.predictSingle(FVec.Transformer.fromArray(featV.toDense.toArray, false))
        (term, doc, pred)
      })

      mapped.orderBy(desc("prediction"))
        .write
        .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .save(args(2) + "." + label + ".predict" )

      // TODO: repartition to 1 in a separate job so as to free executors
      //        .repartition(1)
      //        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")

    })

    spark.stop()
  }
}
