package edu.nyu.tandon

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object test {

  val sparkContext = new SparkContext(new SparkConf().setAppName(this.getClass.toString).setMaster("local[*]"))
  val sqlContext = new SQLContext(sparkContext)

}
