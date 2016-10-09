package edu.nyu.tandon.experiments

import java.io.{File, FileWriter}
import java.util.UUID

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

  trait TemporaryFolder {

    lazy val temporaryFolder = {
      val dir = File.createTempFile("test", "")
      dir.delete
      dir.mkdir
      dir
    }

    /** create a new file in the temp directory */
    def createNewFile = {
      val f = new File(temporaryFolder.getPath + "/" + UUID.randomUUID.toString)
      f.createNewFile
      f
    }

    /** delete each file in the directory and the directory itself */
    def delete = {
      Option(temporaryFolder.listFiles).map(_.toList).getOrElse(Nil).foreach(_.delete)
      temporaryFolder.delete
    }
  }

  trait Strategy {
    val strategy = mock[SelectiveDocumentalIndexStrategy]
    when(strategy.globalPointer(0, 0l)).thenReturn(0l)
    when(strategy.globalPointer(0, 1l)).thenReturn(1l)
    when(strategy.globalPointer(0, 2l)).thenReturn(2l)
    when(strategy.globalPointer(1, 0l)).thenReturn(3l)
    when(strategy.globalPointer(1, 1l)).thenReturn(4l)
    when(strategy.globalPointer(1, 2l)).thenReturn(5l)
    when(strategy.numberOfDocuments(0)).thenReturn(3)
    when(strategy.numberOfDocuments(1)).thenReturn(3)

    def c0 = TranslateToGlobalIds.toGlobal(strategy, 0)_
    def c1 = TranslateToGlobalIds.toGlobal(strategy, 1)_
  }

  trait Results {
    val input = Seq(
      "0 1",
      "1 2",
      "2 0"
    )
    val expected0 = Seq(
      "0 1",
      "1 2",
      "2 0"
    )
    val expected1 = Seq(
      "3 4",
      "4 5",
      "5 3"
    )
  }

//  trait DataFrames {
//    val df = sqlContext.createDataFrame(List(
//      (0, "0 1"),
//      (1, "1 2"),
//      (2, "2 0")
//    )).toDF("id", "results")
//  }

  test("toGlobal") {
    new Strategy {
      assert(c0(Seq(0)) === Seq(0))
      assert(c0(Seq(0, 1, 2)) === Seq(0, 1, 2))
      assert(c1(Seq(0)) === Seq(3))
      assert(c1(Seq(0, 1, 2)) === Seq(3, 4, 5))
    }
  }

  test("translate cluster 0") {

    new Strategy with Results with TemporaryFolder {

      // given
      val f = createNewFile
      val writer = new FileWriter(f)
      for (line <- input) writer.append(s"$line\n")
      writer.close()

      // when
      TranslateToGlobalIds.translate(f, 0, strategy)

      // then
      assertResult(expected0) {
        Source.fromFile(f).getLines().toSeq
      }

    }
  }

  test("translate cluster 1") {

    new Strategy with Results with TemporaryFolder {

      // given
      val f = createNewFile
      val writer = new FileWriter(f)
      for (line <- input) writer.append(s"$line\n")
      writer.close()

      // when
      TranslateToGlobalIds.translate(f, 1, strategy)

      // then
      assertResult(expected1) {
        Source.fromFile(f).getLines().toSeq
      }

    }
  }

}
