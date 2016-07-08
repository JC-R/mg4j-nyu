package edu.nyu.tandon.experiments

import java.io.{File, FileInputStream, ObjectInputStream, PrintWriter}

import edu.nyu.tandon._
import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy
import edu.nyu.tandon.ml.features._
import edu.nyu.tandon.spark.SQLContextSingleton
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
object TranslateToGlobalIds {

  def toGlobal(strategy: SelectiveDocumentalIndexStrategy, cluster: Int)(localIds: Seq[Long]): Seq[Long] =
    localIds map (strategy.globalPointer(cluster, _))

  def columnToGlobal(strategy: SelectiveDocumentalIndexStrategy, cluster: Int) = udf {
    line: String => {
      longsToLine(toGlobal(strategy, cluster)(lineToLongs(line)))
    }
  }

  def translate(input: DataFrame, cluster: Int, strategy: SelectiveDocumentalIndexStrategy): DataFrame = {
    input
      .withColumn("results-global",
        columnToGlobal(strategy, cluster)(input("results")))
      .drop("results")
  }

  def main(args: Array[String]): Unit = {

    case class Config(input: File = null, strategy: File = null, cluster: Int = -1)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[File]('i', "input")
        .action((x, c) => c.copy(input = x))
        .text("result file with local IDs")
        .required()

      opt[File]('s', "strategy")
        .action((x, c) => c.copy(strategy = x))
        .text("strategy according to which the translation is performed")
        .required()

      opt[Int]('c', "cluster")
        .action((x, c) => c.copy(cluster = x))
        .text("cluster number")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val sparkContext = new SparkContext(new SparkConf().setAppName(this.getClass.toString).setMaster("local[*]"))
        val sqlContext = SQLContextSingleton.getInstance(sparkContext)

        val strategy = new ObjectInputStream(new FileInputStream(config.strategy)).readObject()
          .asInstanceOf[SelectiveDocumentalIndexStrategy]

        saveFeatureFile(translate(
          loadFeatureFile(sqlContext)(config.input),
          config.cluster,
          strategy
        ), config.input.getAbsolutePath + ".global")

      case None =>
    }

  }

}
