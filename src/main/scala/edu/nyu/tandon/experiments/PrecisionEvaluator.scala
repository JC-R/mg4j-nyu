package edu.nyu.tandon.experiments

import java.io.{File, FileOutputStream, OutputStreamWriter, Writer}

import edu.nyu.tandon.experiments.Utils._
import edu.nyu.tandon.utils.BulkIterator
import scopt.OptionParser

import scala.io.Source

object PrecisionEvaluator {

  /**
    * Returns the fraction of results from original that also exists in actual.
    *
    * @param original   original set of results
    * @param actual     actual set of results
    * @return           either Some(precision), or None if original is empty
    */
  def precision(original: Seq[Long], actual: Seq[Long]): Option[Double] =
    if (original.isEmpty) None
    else Some((original intersect actual).length.toDouble / original.length.toDouble)

  /**
    * Prints all doubles in iterator and returns the sum of all numbers and its count.
    * Each None value in the iterator is excluded from aggregated numbers, and
    * for each of them the value printed to the output is a dash: -
    *
    * @param iterator iterator of
    * @param writer   where to print the output
    * @return         a pair (sum, count)
    */
  def printAndAggregatePrecision(iterator: Iterator[Option[Double]], writer: Writer): (Double, Int) = {
    val result = iterator
      .map(x => (x, 0))
      .foldLeft((0.0, 0)) {
        case (x, y) => y._1 match {
          case Some(v) => writer.write(v + "\n")
            (x._1 + v, x._2 + 1)
          case None => writer.write("-\n")
            (x._1, x._2)
        }
      }
    writer.flush()
    result
  }

  def evaluatePrecision(fullResultsFile: File,
                        shardChoicesFile: File,
                        shardResultsFiles: Seq[File],
                        outputWriter: OutputStreamWriter) = {

    val fullIterator = Source.fromFile(fullResultsFile).getLines() map lineToLongs
    val shardChoicesIterator = Source.fromFile(shardChoicesFile).getLines() map lineToLongs
    val shardResultsIterators = for (f <- shardResultsFiles) yield Source.fromFile(f).getLines() map lineToLongs

    val uberIterator = new BulkIterator(shardResultsIterators)

    val documentsForQueries = for (shards <- shardChoicesIterator) yield {
      val n = uberIterator.next()
      for (shard <- shards) yield n(shard.toInt)
    }.flatten

    val precisionIterator: Iterator[Option[Double]] = (fullIterator zip documentsForQueries) map {
      case (topResults, clusterResults) => precision(topResults, clusterResults)
    }

    val (sum, len) = printAndAggregatePrecision(precisionIterator, outputWriter)
    println(sum / len.toDouble)
  }

  def main(args: Array[String]) = {

    case class Config(fullResults: File = null,
                      shards: File = null,
                      shardResults: Seq[File] = List(),
                      outputWriter: OutputStreamWriter = new OutputStreamWriter(System.out))

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[File]('f', "full-results")
        .action((x, c) => c.copy(fullResults = x))
        .text("result file of the full index")
        .required()

      opt[File]('s', "shards")
        .action((x, c) => c.copy(shards = x))
        .text("shards chosen for the queries")
        .required()

      opt[Seq[File]]('c', "cluster-results")
        .action((x, c) => c.copy(shardResults = x))
        .text("result files of the cluster shards")
        .required()

      opt[File]('o', "output")
        .action((x, c) => c.copy(outputWriter = new OutputStreamWriter(new FileOutputStream(x))))
        .text("output file")

    }

    parser.parse(args, Config()) match {
      case Some(config) => evaluatePrecision(
        config.fullResults,
        config.shards,
        config.shardResults,
        config.outputWriter)
      case None =>
    }

  }
}
