package edu.nyu.tandon.ml.features

import java.io._

import edu.nyu.tandon._
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
object SegmentCounter {

  val logger = LoggerFactory.getLogger(SegmentCounter.getClass())

  def countByBin(numDocs: Long, numBins: Int)(topResults: Seq[Long]): Map[Int, Int] = {
    def chunkNumber(r: Long): Int = {
      assert(r < numDocs, s"Result $r is not smaller than number of documents $numDocs")
      math.floor(r.toDouble / numDocs.toDouble * numBins.toDouble).toInt
    }
    topResults.groupBy(chunkNumber)
      .mapValues(_.length)
  }

  def expandRow(resultsColumnId: Int, columnCount: Int, numDocs: Long, numBins: Int)(row: String): Seq[String] = {
    val fields: Seq[String] = row.split(",", -1)
    assert(columnCount == fields.length, s"Mismatch in number of columns: $columnCount != ${fields.length}")

    val results = lineToLongs(fields(resultsColumnId))
    val countsByBin = countByBin(numDocs, numBins)(results).withDefaultValue(0)

    for (bin <- 0 until numBins) yield (fields.updated(resultsColumnId, countsByBin(bin)) :+ bin)
      .mkString(",")
  }

  def segment(data: Iterator[String], column: String, numDocs: Long, numBins: Int): Iterator[String] = {
    val header: String = data.next()
    val columns: Seq[String] = header.split(",")
    val resultsColumnId: Int = columns.indexOf(column)
    assert(resultsColumnId >= 0, s"The column to expand ($column) does not exist")
    assert(columns.indexOf(column, resultsColumnId + 1) < 0, s"There is more than one columns $column")
    Seq((columns :+ "bin").mkString(",")).toIterator ++ (data flatMap expandRow(resultsColumnId, columns.length, numDocs, numBins))
  }

  def main(args: Array[String]): Unit = {

    case class Config(input: File = null,
                      numBins: Int = -1,
                      numDocs: Int = -1,
                      column: String = "")

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

      opt[String]('c', "column")
        .action((x, c) => c.copy(column = x))
        .text("the column containing results")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        logger.info(s"Segmenting ${config.input} for ${config.numDocs} documents and ${config.numBins} bins...")
        val output = segment(Source.fromFile(config.input).getLines(),
          config.column,
          config.numDocs,
          config.numBins)

        save(config.input.getAbsolutePath + ".segmented")(output)
        logger.info(s"Segmenting done.")

      case None =>
    }

  }

}
