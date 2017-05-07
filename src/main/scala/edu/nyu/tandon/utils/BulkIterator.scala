package edu.nyu.tandon.utils

/**
  * @author michal.siedlaczek@nyu.edu
  */
class BulkIterator[T](val iterators: Seq[Iterator[T]]) extends Iterator[Seq[T]] {

  override def hasNext: Boolean = iterators map (_.hasNext) reduce (_ && _)

  override def next(): Seq[T] = for (i <- iterators) yield i.next()

}
