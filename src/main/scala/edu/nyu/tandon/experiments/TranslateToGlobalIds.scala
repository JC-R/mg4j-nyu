package edu.nyu.tandon.experiments

import java.io.{File, FileInputStream, ObjectInputStream, PrintWriter}

import edu.nyu.tandon._
import edu.nyu.tandon.ml.features._
import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy
import edu.nyu.tandon.spark.SQLContextSingleton
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
object TranslateToGlobalIds {

  def toGlobal(strategy: SelectiveDocumentalIndexStrategy, cluster: Int)(localIds: Seq[Long]): Seq[Long] =
    localIds map (strategy.globalPointer(cluster, _))

  def translate(input: File, cluster: Int, strategy: SelectiveDocumentalIndexStrategy): Unit = {
    val globalFile = new File(input.getAbsolutePath + ".global")
    val writer = new PrintWriter(globalFile)
    val lineStream = Source.fromFile(input).getLines().toStream
    for {
      line <- lineStream map lineToLongs map toGlobal(strategy, cluster) map longsToLine
    } writer.write(line + "\n")
    writer.close()
  }

//  def translate(sqlContext: SQLContext)(input: File, cluster: Int, strategy: SelectiveDocumentalIndexStrategy): Unit = {
//    val localResultsDF = loadFeatureFile(sqlContext)(input)
//    SegmentCounter.segment(localResultsDF, "results", strategy.numberOfDocuments(cluster), 10)
//  }

  def main(args: Array[String]) = {

    case class Config(input: File = null, strategy: File = null, cluster: Int = -1)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[File]('i', "input")
        .action( (x, c) => c.copy(input = x) )
        .text("result file with local IDs")
        .required()

      opt[File]('s', "strategy")
        .action( (x, c) => c.copy(strategy = x) )
        .text("strategy according to which the translation is performed")
        .required()

      opt[Int]('c', "cluster")
        .action( (x, c) => c.copy(cluster = x) )
        .text("cluster number")
        .required()

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val strategy = new ObjectInputStream(new FileInputStream(config.strategy)).readObject()
          .asInstanceOf[SelectiveDocumentalIndexStrategy]

        translate(config.input, config.cluster, strategy)

      case None =>
    }

  }

}
