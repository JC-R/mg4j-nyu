package edu.nyu.tandon.ml.features

import java.io.File

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object FeatureJoin {

  def loadFeatureFile(sqlContext: SQLContext)(file: File): DataFrame =
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(file.getAbsolutePath)

  def saveFeatureFile(features: DataFrame, file: String): Unit =
    features.write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .save(file)

  def join(features: Seq[DataFrame]): DataFrame =
    features reduce (_.join(_, "id"))

  def join(sqlContext: SQLContext)(features: Seq[File]): DataFrame =
    join(features map loadFeatureFile(sqlContext))

  def main(args: Array[String]): Unit = {

    case class Config(features: Seq[File] = null,
                      output: File = null)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[Seq[File]]('f', "features")
        .action((x, c) => c.copy(features = x))
        .text("the list of files containing features")
        .required()

      opt[File]('o', "output")
        .action((x, c) => c.copy(output = x))
        .text("the output file")
        .required()

    }

    parser.parse(args, Config()) match {
      case None =>
      case Some(config) =>

        val sparkContext = new SparkContext(new SparkConf().setAppName(this.getClass.toString).setMaster("local[*]"))
        val sqlContext = new SQLContext(sparkContext)

        saveFeatureFile(
          join(sqlContext)(config.features),
          config.output.getAbsolutePath
        )

    }

  }

}
