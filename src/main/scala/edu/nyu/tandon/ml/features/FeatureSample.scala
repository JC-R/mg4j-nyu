package edu.nyu.tandon.ml.features

import java.io.File

import edu.nyu.tandon.spark.SQLContextSingleton
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object FeatureSample {

  def sample(input: DataFrame, fraction: Double): DataFrame =
    input.sample(false, fraction)

  def main(args: Array[String]): Unit = {

    case class Config(input: File = null,
                      output: File = null)

    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[File]('i', "input")
        .action((x, c) => c.copy(input = x))
        .text("the input file")
        .required()

      opt[File]('o', "output")
        .action((x, c) => c.copy(output = x))
        .text("the output file")
        .required()

    }

    parser.parse(args, Config()) match {
      case None =>
      case Some(config) =>

        val sparkContext = new SparkContext(new SparkConf()
          .setAppName(this.getClass.toString))
        val sqlContext = SQLContextSingleton.getInstance(sparkContext)



    }

  }

}
