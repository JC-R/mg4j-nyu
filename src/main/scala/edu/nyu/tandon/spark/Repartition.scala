package edu.nyu.tandon.spark

import org.apache.spark.sql._

/**
  * Created by juan on 1/1/17.
  */
object Repartition {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("\nUse: Partition <DF> <partitions> <newDF>\n")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Partition-"+args(0))
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //      .repartition(spark.sparkContext.defaultParallelism * 3)
    spark
      .read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .parquet(args(0))
      .repartition(args(1).toInt)
      .write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .parquet(args(2))
  }
}
