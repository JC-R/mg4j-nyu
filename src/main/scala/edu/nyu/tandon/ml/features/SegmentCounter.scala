package edu.nyu.tandon.ml.features

import java.io._

import edu.nyu.tandon._
import edu.nyu.tandon.ml._
import edu.nyu.tandon.spark.SQLContextSingleton
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object SegmentCounter {

  def countByBin(numDocs: Long, numBins: Int)(topResults: Seq[Long]): Map[Int, Int] = {
    def chunkNumber(r: Long): Int = {
      assert(r < numDocs)
      math.floor(r.toDouble / numDocs.toDouble * numBins.toDouble).toInt
    }
    topResults.groupBy(chunkNumber)
      .mapValues(_.length)
  }

  def binsToRows(numBins: Int)(id: Long,chunks: Map[Int, Int]): Seq[(Int, Int)] = {
    val c = chunks.withDefaultValue(0)
    for (i <- 0 until numBins) yield (i, c(i))
  }

  def segment(data: DataFrame, column: String, numDocs: Long, numBins: Int): DataFrame = {
    data.explode(data(IdCol), data(column)) {
      case Row(id: Int, line: String) =>
        binsToRows(numBins)(id, countByBin(numDocs, numBins)(lineToLongs(line.toString)))
    }
      .drop(column)
      .withColumnRenamed("_1", "segment")
      .withColumnRenamed("_2", "count")
  }

  def main(args: Array[String]): Unit = {

    case class Config(input: File = null,
                      numBins: Int = -1,
                      numDocs: Int = -1,
                      sparkMaster: String = "local[*]")

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[File]('i', "input")
        .action((x, c) => c.copy(input = x))
        .text("result file with local IDs")
        .required()

      opt[Int]('b', "num-bins")
        .action((x, c) => c.copy(numBins = x))
        .text("the number of bins to segment a cluster to")
        .required()

      opt[Int]('d', "num-docs")
        .action((x, c) => c.copy(numDocs = x))
        .text("the number of documents")
        .required()

      opt[String]('M', "spark-master")
        .action((x, c) => c.copy(sparkMaster = x))
        .text("spark master (default: local[*])")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        val sparkContext = new SparkContext(new SparkConf()
          .setAppName(this.getClass.toString)
          .setMaster(config.sparkMaster))
        val sqlContext = SQLContextSingleton.getInstance(sparkContext)

        val output = segment(loadFeatureFile(sqlContext)(config.input),
          "results",
          config.numDocs,
          config.numBins)

        saveFeatureFile(output, config.input.getAbsolutePath + ".segmented")

      case None =>
    }

  }

}
