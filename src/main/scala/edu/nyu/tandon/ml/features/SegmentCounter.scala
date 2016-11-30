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

  def main(args: Array[String]): Unit = {

    case class Config(input: File = null,
                      numBins: Int = -1,
                      numDocs: Int = -1,
                      cluster: Int = -1)

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

      opt[Int]('c', "cluster")
        .action((x, c) => c.copy(cluster = x))
        .text("cluster number")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        val sparkContext = new SparkContext(new SparkConf().setAppName(this.getClass.toString).setMaster("local[*]"))
        val sqlContext = SQLContextSingleton.getInstance(sparkContext)

        val output = segment(loadFeatureFile(sqlContext)(config.input),
          "results",
          config.numDocs,
          config.numBins,
          config.cluster)

        saveFeatureFile(output, config.input.getAbsolutePath + ".segmented")

      case None =>
    }

  }

  def segment(data: DataFrame, column: String, numDocs: Long, numBins: Int, cluster: Int): DataFrame = {
    data.explode(data(IdCol), data(column)) {
      case Row(id: Int, line: String) =>
        binsToRows(numBins)(id, cluster, countByBin(numDocs, numBins)(lineToLongs(line.toString)))
    }
      .drop(column)
      .withColumnRenamed("_1", "segment")
      .withColumnRenamed("_2", "count")
  }

  def countByBin(numDocs: Long, numBins: Int)(topResults: Seq[Long]): Map[Int, Int] = {
    def chunkNumber(r: Long): Int = {
      assert(r < numDocs)
      math.floor(r.toDouble / numDocs.toDouble * numBins.toDouble).toInt
    }
    topResults.groupBy(chunkNumber)
      .mapValues(_.length)
  }

  def binsToRows(numBins: Int)(id: Long, cluster: Int, chunks: Map[Int, Int]): Seq[(Int, Int)] = {
    val c = chunks.withDefaultValue(0)
    for (i <- 0 until numBins) yield (i, c(i))
  }

}
