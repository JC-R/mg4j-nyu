package edu.nyu.tandon.experiments.ml

import edu.nyu.tandon.experiments.ml.TranslateToGlobalIds
import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy
import edu.nyu.tandon.test._
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar._

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
    when(strategy.numberOfDocuments(0)).thenReturn(3)
    when(strategy.numberOfDocuments(1)).thenReturn(3)

    def c0 = TranslateToGlobalIds.toGlobal(strategy, 0) _

    def c1 = TranslateToGlobalIds.toGlobal(strategy, 1) _
  }

  trait DataFrames {
    val df = sqlContext.createDataFrame(List(
      (0, "0 1"),
      (1, "1 2"),
      (2, "2 0")
    )).toDF("id", "results")
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
    new Strategy with DataFrames {
      assertResult(
        sqlContext.createDataFrame(List(
          (0, "0 1"),
          (1, "1 2"),
          (2, "2 0")
        )).toDF("id", "results-global").head(3)
      ) {
        TranslateToGlobalIds.translate(df, 0, strategy).head(3)
      }
    }
  }

  test("translate cluster 1") {
    new Strategy with DataFrames {
      assertResult(
        sqlContext.createDataFrame(List(
          (0, "3 4"),
          (1, "4 5"),
          (2, "5 3")
        )).toDF("id", "results-global").head(3)
      ) {
        TranslateToGlobalIds.translate(df, 1, strategy).head(3)
      }
    }
  }

}
