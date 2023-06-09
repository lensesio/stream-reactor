package io.lenses.streamreactor.connect.io.text

import scala.annotation.tailrec

object TextUtils {

  /*
   Checks if a subset of source string is contained at the end of target string.
   */
  def partiallyEndsWith(target: String, source: String): Option[String] =
    //it can be that only a subset of the source string is contained at the end of the target string
    //e.g. target = "abcde" and source = "cde" or target = "abc" and source = "bcde" or
    //target = "aaab" and source = "bc"
    //so we need to check for this
    if (target.isEmpty || source.isEmpty) None
    else {
      val targetIndex = target.length - 1
      inner(target, targetIndex, source)
    }

  @tailrec
  private def inner(target: String, targetIndex: Int, source: String): Option[String] =
    if (targetIndex < 0) None
    else {
      var targetStartIndex = targetIndex
      var sourceStartIndex = 0
      var matches          = true
      while (sourceStartIndex < source.length && targetStartIndex < target.length && matches) {
        matches = source(sourceStartIndex) == target(targetStartIndex)
        sourceStartIndex += 1
        targetStartIndex += 1
      }
      if (matches) Some(target.substring(targetIndex))
      else inner(target, targetIndex - 1, source)
    }
}
