package edu.nyu.tandon.utils

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * @author michal.siedlaczek@nyu.edu
  */
@RunWith(classOf[JUnitRunner])
class BulkIteratorTest extends FunSuite {

  trait Iterators {
    val iterators = List(
      Array(1, 2, 3).toIterator,
      Array(1, 99).toIterator,
      Array(1, 2).toIterator
    )
    val uberIterator = new BulkIterator(iterators)
  }

  test("hasNext") {
    new Iterators {
      assert(uberIterator.hasNext === true)
      uberIterator.next()
      assert(uberIterator.hasNext === true)
      uberIterator.next()
      assert(uberIterator.hasNext === false)
    }
  }

  test("next") {
    new Iterators {
      assert(uberIterator.next === List(1,1,1))
      assert(uberIterator.next === List(2,99,2))
    }
  }

}
