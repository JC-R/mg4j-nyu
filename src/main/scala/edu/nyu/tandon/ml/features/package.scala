package edu.nyu.tandon.ml

import java.io.File

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.DoubleType

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object features {

  private val TempSuffix = "___TMP___"

  def loadFeatureFile(sqlContext: SQLContext)(file: File): DataFrame =
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(file.getAbsolutePath)

  def saveFeatureFile(features: DataFrame, file: String): Unit =
    features.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(file)

  def withColumnRenamedAndCastedToDouble(dataFrame: DataFrame, columnName: String): DataFrame =
    dataFrame
      .withColumn(columnName + TempSuffix, dataFrame(columnName).cast(DoubleType))
      .drop(columnName)
      .withColumnRenamed(columnName + TempSuffix, columnName)

  def convertColumnsToDouble(data: DataFrame): DataFrame =
    data.columns.foldLeft(data)(withColumnRenamedAndCastedToDouble)

}
