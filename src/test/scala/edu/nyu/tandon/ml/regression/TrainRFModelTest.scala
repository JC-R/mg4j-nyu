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
class TrainRFModelTest extends FunSuite {

//  val sparkContext = new SparkContext(new SparkConf().setAppName(this.getClass.toString).setMaster("local[*]"))
//  val sqlContext = new SQLContext(sparkContext)

  trait DataFrames {
    val df = sqlContext.createDataFrame(List(
      (0, 0.0, 1.0),
      (1, 0.1, 1.1)
    )).toDF("id", "f1", "f2")
  }

  test("featureAssembler") {
    new DataFrames {
      val transformedData = TrainRFModel.featureAssembler(df).transform(df)
      val head = transformedData.select("features").take(2)
      assert(head(0) === Row.fromSeq(List(Vectors.dense(0.0, 1.0))))
      assert(head(1) === Row.fromSeq(List(Vectors.dense(0.1, 1.1))))
      intercept[IllegalStateException] {
        TrainRFModel.featureAssembler(transformedData).transform(transformedData)
      }
    }
  }



}
