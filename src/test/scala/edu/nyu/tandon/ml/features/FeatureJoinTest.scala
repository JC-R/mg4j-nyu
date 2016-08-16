package edu.nyu.tandon.ml.features

import java.io.{File, FileWriter}

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner
import edu.nyu.tandon.test._

/**
  * @author michal.siedlaczek@nyu.edu
  */
@RunWith(classOf[JUnitRunner])
class FeatureJoinTest extends FunSuite with BeforeAndAfterAll {

  trait Files {
    val f1 = File.createTempFile("first", "feature")
    val f2 = File.createTempFile("second", "feature")
    new FileWriter(f1)
      .append("id,f1\n")
      .append("0,0.1\n")
      .append("1,0.2\n")
      .close()
    new FileWriter(f2)
      .append("id,f2\n")
      .append("0,0.3\n")
      .append("1,0.4\n")
      .append("1,0.5\n")
      .close()
  }

  test("join") {
    // given
    new Files {
      // when
      val joined = FeatureJoin.join(sqlContext, Seq("id"))(List(f1, f2)).sort("id", "f1", "f2")
      // then
      assertResult(1) { joined.filter(joined("id") === 0).count() }
      assertResult(2) { joined.filter(joined("id") === 1).count() }
      assertResult(Row.fromTuple((0, 0.1, 0.3))) { joined.filter(joined("id") === 0).first() }
      assertResult(Row.fromTuple((1, 0.2, 0.4))) { joined.filter(joined("id") === 1).first() }
      assertResult(Row.fromTuple((1, 0.2, 0.5))) { joined.filter(joined("id") === 1).take(2)(1) }
      assertResult(0) { joined.filter(!joined("id").isin(0, 1)).count() }
    }
  }

}
