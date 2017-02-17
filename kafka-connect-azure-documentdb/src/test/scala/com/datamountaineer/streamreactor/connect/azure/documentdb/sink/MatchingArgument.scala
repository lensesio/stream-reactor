package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import org.mockito.{ArgumentMatcher, ArgumentMatchers}


trait MatchingArgument {
  def argThat[T](thunk: T => Boolean): T = ArgumentMatchers.argThat {
    new ArgumentMatcher[T] {
      override def matches(argument: T): Boolean = thunk(argument)
    }
  }
}
