package com.datamountaineer.streamreactor.connect

object IteratorToSeqFn {
  def apply[T](iter: java.util.Iterator[T]): Seq[T] = {
    new Iterator[T]() {
      override def hasNext: Boolean = iter.hasNext

      override def next(): T = iter.next()
    }.toSeq
  }
}
