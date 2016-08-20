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

    case class Config(input: File = null,
                      strategy: File = null,
                      cluster: Int = -1,
                      sparkMaster: String = "local[*]")

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

      opt[String]('M', "spark-master")
        .action((x, c) => c.copy(sparkMaster = x))
        .text("spark master (default: local[*])")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val sparkContext = new SparkContext(
          new SparkConf().setAppName(this.getClass.toString).setMaster(config.sparkMaster)
        )
        val sqlContext = SQLContextSingleton.getInstance(sparkContext)

        val strategy = new ObjectInputStream(new FileInputStream(config.strategy)).readObject()
          .asInstanceOf[SelectiveDocumentalIndexStrategy]

        try {
          val translated = translate(
            loadFeatureFile(sqlContext)(config.input),
            config.cluster,
            strategy
          )

          saveFeatureFile(translated, config.input.getAbsolutePath + ".global")
        }
        catch {
          case e: Exception => throw new RuntimeException(
            "A fatal error occurred while processing " +
              s"input=${config.input.getAbsolutePath()}; " +
              s"strategy=${config.strategy.getAbsolutePath()}; " +
              s"cluster=${config.cluster}",
            e)
        }

      case None =>
    }

  }

}
