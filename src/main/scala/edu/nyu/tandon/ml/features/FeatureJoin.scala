package edu.nyu.tandon.ml.features

import java.io.File

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import edu.nyu.tandon.spark.SQLContextSingleton

/**
  * @author michal.siedlaczek@nyu.edu
  */
object FeatureJoin {

  def join(joinCols: Seq[String], features: Seq[DataFrame]): DataFrame =
    features reduce (_.join(_, "id"))

  def join(joinCols: Seq[String], head: DataFrame, rest: DataFrame*): DataFrame =
    join(joinCols, head +: rest)

  def join(sqlContext: SQLContext, joinCols: Seq[String])(features: Seq[File]): DataFrame =
    join(joinCols, features map loadFeatureFile(sqlContext))

  def main(args: Array[String]): Unit = {

    case class Config(features: Seq[File] = null,
                      output: File = null,
                      joinCols: Seq[String] = null)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[Seq[File]]('f', "features")
        .action((x, c) => c.copy(features = x))
        .text("the list of files containing features")
        .required()

      opt[File]('o', "output")
        .action((x, c) => c.copy(output = x))
        .text("the output file")
        .required()

      opt[Seq[String]]('j', "join-cols")
        .action((x, c) => c.copy(joinCols = x))
        .text("the join colums")
        .required()

    }

    parser.parse(args, Config()) match {
      case None =>
      case Some(config) =>

        val sparkContext = new SparkContext(new SparkConf().setAppName(this.getClass.toString).setMaster("local[*]"))
        val sqlContext = SQLContextSingleton.getInstance(sparkContext)

        saveFeatureFile(
          join(sqlContext, config.joinCols)(config.features),
          config.output.getAbsolutePath
        )

    }

  }

}
