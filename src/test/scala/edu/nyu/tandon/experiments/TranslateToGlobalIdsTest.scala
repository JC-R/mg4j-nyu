package edu.nyu.tandon.experiments

import java.io.{File, FileWriter}

import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar._

import scala.io.Source

/**
  * @author michal.siedlaczek@nyu.edu
  */
@RunWith(classOf[JUnitRunner])
class TranslateToGlobalIdsTest extends FunSuite {

  trait Strategy {
    val strategy = mock[SelectiveDocumentalIndexStrategy]
    when(strategy.globalPointer(0, 0l)).thenReturn(0l)
    when(strategy.globalPointer(0, 1l)).thenReturn(1l)
    when(strategy.globalPointer(0, 2l)).thenReturn(2l)
    when(strategy.globalPointer(1, 0l)).thenReturn(3l)
    when(strategy.globalPointer(1, 1l)).thenReturn(4l)
    when(strategy.globalPointer(1, 2l)).thenReturn(5l)

    def c0 = TranslateToGlobalIds.toGlobal(strategy, 0)_
    def c1 = TranslateToGlobalIds.toGlobal(strategy, 1)_
  }

  trait TemporaryFiles {
    val input = File.createTempFile("input", "results")
    val output = new File(input.getAbsolutePath + ".global")

    def writeLinesToFile(file: File, lines: Seq[String]) = {
      val writer = new FileWriter(file)
      for (line <- lines) writer.write(line + "\n")
      writer.close()
    }

    def readLinesFromFile(file: File): Seq[String] = {
      (for (line <- Source.fromFile(file).getLines()) yield line) toList
    }
  }

  test("toGlobal") {
    new Strategy {
      assert(c0(Seq(0)) === Seq(0))
      assert(c0(Seq(0, 1, 2)) === Seq(0, 1, 2))
      assert(c1(Seq(0)) === Seq(3))
      assert(c1(Seq(0, 1, 2)) === Seq(3, 4, 5))
    }
  }

  test("translate cluster 0") {
    new Strategy with TemporaryFiles {
      // given
      writeLinesToFile(input, List(
        "0 1",
        "1 2",
        "2 0"
      ))

      // when
      TranslateToGlobalIds.translate(input, 0, strategy)

      // then
      assert(readLinesFromFile(output) === Seq(
        "0 1",
        "1 2",
        "2 0"
      ))
    }
  }

  test("translate cluster 1") {
    new Strategy with TemporaryFiles {
      // given
      writeLinesToFile(input, List(
        "0 1",
        "1 2",
        "2 0"
      ))

      // when
      TranslateToGlobalIds.translate(input, 1, strategy)

      // then
      assert(readLinesFromFile(output) === Seq(
        "3 4",
        "4 5",
        "5 3"
      ))
    }
  }

}
