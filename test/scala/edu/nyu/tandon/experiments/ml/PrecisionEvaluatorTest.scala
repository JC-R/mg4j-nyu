package edu.nyu.tandon.experiments.ml

import java.io.StringWriter

import edu.nyu.tandon.experiments.ml.PrecisionEvaluator
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * @author michal.siedlaczek@nyu.edu
  */
@RunWith(classOf[JUnitRunner])
class PrecisionEvaluatorTest extends FunSuite {

  test("precision") {
    // given
    val original: Seq[Long] = Seq(1, 2, 3, 4)
    val actual: Seq[Long] = Seq(1, 3)

    // when
    val precision = PrecisionEvaluator.precision(original, actual)

    // then
    assert(precision === Some(0.5))
  }

  test("precision with fixed k") {
    // given
    val original: Seq[Long] = Seq(1, 2, 3, 4)
    val actual: Seq[Long] = Seq(1, 3)

    // when
    val precision = PrecisionEvaluator.precision(original, actual, k = 10)

    // then
    assert(precision === Some(0.2))
  }

  test("undefined precision") {
    // given
    val original: Seq[Long] = Seq()
    val actual: Seq[Long] = Seq(1, 3)

    // when
    val precision = PrecisionEvaluator.precision(original, actual)

    // then
    assert(precision === None)
  }

  test("printAndAggregatePrecision") {
    // given
    val iterator = Seq(Some(1.0), Some(2.0), None, Some(4.0), Some(5.0)).toIterator
    val writer = new StringWriter()

    // when
    val (sum, count) = PrecisionEvaluator.printAndAggregatePrecision(iterator, writer)

    // then
    assert(sum === 12.0)
    assert(count === 4)
    assert(writer.toString === "1.0\n2.0\n-\n4.0\n5.0\n")
  }

}
