package edu.nyu.tandon.ml.regression

import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import edu.nyu.tandon.test._

/**
  * @author michal.siedlaczek@nyu.edu
  */
@RunWith(classOf[JUnitRunner])
class RFRegressionTest extends FunSuite {

  trait DataFrames {
    val r = new RFRegression(numTrees = 10, maxBins = 10, maxDepth = 10, labelCol = "label")
    val df = sqlContext.createDataFrame(List(
      (0, 0.0, 1.0),
      (1, 0.1, 1.1)
    )).toDF("id", "f1", "f2")
  }

  test("featureAssembler") {
    new DataFrames {
      val transformedData = r.featureAssembler(df).transform(df)
      val head = transformedData.select("features").take(2)
      val q = Row.fromSeq(List(Vectors.dense(0.0, 1.0)))
      assert(head(0).mkString(" ") === List(Vectors.dense(0.0, 1.0)).mkString(" "))
      assert(head(1).mkString(" ") === List(Vectors.dense(0.1, 1.1)).mkString(" "))
      intercept[IllegalStateException] {
        r.featureAssembler(transformedData).transform(transformedData)
      }
    }
  }



}
