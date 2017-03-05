package edu.nyu.tandon.experiments

import java.io._
import java.nio.file.{Files, Paths}

import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy
import scopt.OptionParser

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
object TranslateToGlobalIds {

  def toGlobal(strategy: DocumentalClusteringStrategy, cluster: Int)(localIds: Seq[Long]): Seq[Long] =
    localIds map (strategy.globalPointer(cluster, _))

  def translate(input: File, cluster: Int, strategy: DocumentalClusteringStrategy): Unit = {
    /*
     * Move the current file to temp file.
     * We're going to replace the old file with the new one
     * containing global IDs.
     */
    val localFile = input.toPath
    val globalFile = Paths.get(input.toString.replace("local", "global"))
    Files.move(localFile, localFile)

    /* Translate */
    val globalIds = Source.fromFile(localFile.toFile).getLines()
      .map(_.split("\\s+").filter(_.length > 0).map(_.toLong).toSeq)
      .map(toGlobal(strategy, cluster))

    /* Write to the original file */
    val writer = new FileWriter(globalFile.toFile)
    try {
      for (line <- globalIds) writer.append(line.mkString(" ")).append("\n")
    } finally {
      writer.close()
    }
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

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val strategy = new ObjectInputStream(new FileInputStream(config.strategy)).readObject()
          .asInstanceOf[DocumentalClusteringStrategy]

        try {
          translate(config.input, config.cluster, strategy)
        }
        catch {
          case e: Exception => throw new RuntimeException(
            "A fatal error occurred while processing " +
              s"input=${config.input.getAbsolutePath}; " +
              s"strategy=${config.strategy.getAbsolutePath}; " +
              s"cluster=${config.cluster}",
            e)
        }

      case None =>
    }

  }

}
