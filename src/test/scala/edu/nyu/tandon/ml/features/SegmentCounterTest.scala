package edu.nyu.tandon.ml.features

import java.io.{ByteArrayOutputStream, OutputStreamWriter}

import edu.nyu.tandon.test._
import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
@RunWith(classOf[JUnitRunner])
class SegmentCounterTest extends FunSuite {

  test("countByBin exhaustive") {
    // given
    val maxDoc = 3
    val numBins = 3
    val topResults = List(0L, 1L, 2L)

    // when
    val counts = SegmentCounter.countByBin(maxDoc, numBins)(topResults)

    // then
    assert(counts === Map(0 -> 1, 1 -> 1, 2 -> 1))
  }

  test("countByBin") {
    // given
    val numDocs = 100
    val numBins = 10
    val topResults = List(17L, 55L, 59L, 99L)

    // when
    val counts = SegmentCounter.countByBin(numDocs, numBins)(topResults)

    // then
    assert(counts === Map(1 -> 1, 5 -> 2, 9 -> 1))
  }

  test("countByBin assert") {
    intercept[AssertionError] {
      SegmentCounter.countByBin(10, 10)(List(10))
    }
  }

  test("segment") {
    // given
    val numDocs = 100
    val numBins = 4
    val it = Seq(
      "id,results",
      "0,1 2 10 54",
      "1,48 77 89",
      "2,"
    ).toIterator

    // when
    val segmented = SegmentCounter.segment(it, "results", numDocs, numBins).toList

    // then
    assert(segmented === List(
      "id,results",
      "0,3",
      "0,0",
      "0,1",
      "0,0",
      "1,0",
      "1,1",
      "1,0",
      "1,2",
      "2,0",
      "2,0",
      "2,0",
      "2,0"
    ))
  }

}
