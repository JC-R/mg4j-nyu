package edu.nyu.tandon.ml.features

import java.io.{ByteArrayOutputStream, OutputStreamWriter}

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

  test("binsToRow") {

    val numChunks = 10
    val chunks = Map(0 -> 1, 2 -> 3, 9 -> 5, 2000 -> 111)

    assertResult (Seq(
      (0, 1, 1),
      (0, 1, 0),
      (0, 1, 3),
      (0, 1, 0),
      (0, 1, 0),
      (0, 1, 0),
      (0, 1, 0),
      (0, 1, 0),
      (0, 1, 0),
      (0, 1, 5)
    )) {
      SegmentCounter.binsToRows(numChunks)(0, 1, chunks)
    }
  }

//  test("segment") {
//    // given
//    val numDocs = 100
//    val numBins = 4
//    val in = Source.createBufferedSource(IOUtils.toInputStream("1 2 10 54\n48 77 89"))
//    val buffer = new ByteArrayOutputStream();
//    val out = new OutputStreamWriter(buffer)
//
//    // when
//    SegmentCounter.segment(in)(out)(numDocs, numBins)
//
//    // then
//    assert(buffer.toString() === "3 0 1 0\n0 1 0 2\n")
//  }

}
