package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.mongodb.client.MongoIterable

object MongoIterableFn {
  def apply[T](mongoIterator: MongoIterable[T]): Seq[T] = {
    val iter = mongoIterator.iterator()
    new Iterator[T]{
      override def hasNext: Boolean = iter.hasNext

      override def next(): T = iter.next()
    }.toSeq
  }
}
