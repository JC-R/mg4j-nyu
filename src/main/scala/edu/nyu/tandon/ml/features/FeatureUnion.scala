package edu.nyu.tandon.ml.features

import java.io.File

import edu.nyu.tandon.spark.SQLContextSingleton
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object FeatureUnion {

  def union(features: Seq[DataFrame]): DataFrame =
    features reduce (_.union(_))

  def union(head: DataFrame, rest: DataFrame*): DataFrame =
    union(head +: rest)

  def union(sqlContext: SQLContext)(features: Seq[File]): DataFrame =
    union(features map loadFeatureFile(sqlContext))

  def main(args: Array[String]): Unit = {

    case class Config(features: Seq[File] = null,
                      output: File = null,
                      sparkMaster: String = "local[*]")

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[Seq[File]]('f', "features")
        .action((x, c) => c.copy(features = x))
        .text("the list of files containing features")
        .required()

      opt[File]('o', "output")
        .action((x, c) => c.copy(output = x))
        .text("the output file")
        .required()

      opt[String]('M', "spark-master")
        .action((x, c) => c.copy(sparkMaster = x))
        .text("spark master (default: local[*])")

    }

    parser.parse(args, Config()) match {
      case None =>
      case Some(config) =>

        val sparkContext = new SparkContext(new SparkConf()
          .setAppName(this.getClass.toString)
          .setMaster(config.sparkMaster))
        val sqlContext = SQLContextSingleton.getInstance(sparkContext)

        saveFeatureFile(
          union(sqlContext)(config.features),
          config.output.getAbsolutePath
        )

    }

  }

}
