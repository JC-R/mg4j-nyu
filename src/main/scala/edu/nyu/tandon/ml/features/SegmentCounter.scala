package edu.nyu.tandon.ml.features

import java.io.File

import edu.nyu.tandon._
import edu.nyu.tandon.ml._
import scopt.OptionParser
import java.io.Writer
import java.io.OutputStreamWriter

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.io.Source
import scala.io.BufferedSource

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

  def binsToString(numBins: Int)(chunks: Map[Int, Int]): String = {
    val c = chunks.withDefaultValue(0)
    (for (i <- 0 until numBins) yield c(i)) mkString " "
  }

  def binsToRows(numBins: Int)(id: Long, chunks: Map[Int, Int]): Seq[(Long, Int)] = {
    val c = chunks.withDefaultValue(0)
    for (i <- 0 until numBins) yield (id, c(i))
  }

  def segment(in: BufferedSource)(out: Writer)(numDocs: Long, numBins: Int): Unit = {
    val results = in.getLines() map lineToLongs
    val counts = results map countByBin(numDocs, numBins)
    for (c <- counts) {
      out.append(binsToString(numBins)(c))
      out.append("\n")
      out.flush()
    }
  }

  def segment(data: DataFrame, column: String, numDoc: Long, numBins: Int): DataFrame = {
    data.explode(data(IdCol), data(column)) {
      case Row(id: Long, line: String) =>
        binsToRows(numBins)(id, countByBin(numDocs, numBins)(lineToLongs(line.toString)))
    }
  }

  def main(args: Array[String]): Unit = {

    case class Config(inputFile: File = null,
                      outputFile: File = null,
                      numBins: Int = -1,
                      numDocs: Int = -1)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[Int]('b', "num-bins")
        .action((x, c) => c.copy(numBins = x))
        .text("the number of bins to segment a cluster to")
        .required()

      opt[Int]('d', "num-docs")
        .action((x, c) => c.copy(numDocs = x))
        .text("the number of documents")
        .required()

    }

    parser.parse(args, Config()) match {
      case None =>
      case Some(config) => segment(Source.stdin)(new OutputStreamWriter(System.out))(config.numDocs, config.numBins)
    }

  }

}
