package com.datamountaineer.streamreactor.connect.elastic.indexname

import scala.annotation.tailrec

class InvalidCustomIndexNameException(message: String) extends RuntimeException(message)

case class CustomIndexName(fragments: Vector[IndexNameFragment]) {
  override def toString: String = fragments.map(_.getFragment).mkString
}

object CustomIndexName {

  @tailrec
  private def parseIndexName(remainingChars: Vector[Char], currentFragment: StringBuilder, results: Vector[Option[IndexNameFragment]]): Vector[IndexNameFragment] =
    remainingChars match {
      case head +: rest => head match {
        case DateTimeFragment.OpeningChar =>
          val (dateTimeFormat, afterDateTimeFormatIncludingClosingChar) = rest.span { _ != DateTimeFragment.ClosingChar }
          val afterDateTimeFormat = afterDateTimeFormatIncludingClosingChar.tail

          val maybeCurrentFragment = currentFragment.mkString.toOption
          val maybeDateTimeFormat = dateTimeFormat.mkString.toOption

          val newResultsWithDateTimeFragment = results :+ maybeCurrentFragment.map(TextFragment.apply) :+ maybeDateTimeFormat.map(DateTimeFragment(_))

          parseIndexName(afterDateTimeFormat, new StringBuilder, newResultsWithDateTimeFragment)
        case DateTimeFragment.ClosingChar => throw new InvalidCustomIndexNameException(s"Found closing '${DateTimeFragment.ClosingChar}' but no opening character")
        case anyOtherChar => parseIndexName(rest, currentFragment.append(anyOtherChar), results)
      }
      case Vector() =>
        val maybeCurrentFragment = currentFragment.mkString.toOption
        (results :+ maybeCurrentFragment.map(TextFragment.apply)).flatten
    }

  def parseIndexName(indexName: String): CustomIndexName =
    CustomIndexName(parseIndexName(indexName.toVector, new StringBuilder, Vector.empty))
}
