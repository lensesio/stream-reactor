package io.lenses.streamreactor.connect.io.text

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

/**
  * Reads records from an input stream. A record is identified as the data contained between the prefix and suffix.
  * The prefix and suffix are included in the line.
  * @param input
  * @param prefix
  * @param suffix
  */
class PrefixSuffixLineReader(input: InputStream, prefix: String, suffix: String, linesSkip: Int = 0) {
  private val br         = new BufferedReader(new InputStreamReader(input))
  private val suffixSize = suffix.length
  private var currentLine: String = ""

  if (linesSkip > 0) {
    for (_ <- 0 until linesSkip) {
      br.readLine()
    }
  }

  //Returns the next record or None if there are no more
  def next(): Option[String] =
    //read until we find the prefix. If we don't find it, return None
    readUntilPrefixOrNone()
      .flatMap {
        case PrefixReadResult(line, index) =>
          //check if the suffix is on the same line as the prefix. Grab the content between suffix and prefix as the value
          // and keep the remaining content for the next time next() is called. Else read until the suffix is found
          val suffixIndex = line.indexOf(suffix, index)
          if (suffixIndex > 0) {
            currentLine = line.substring(suffixIndex + suffixSize)
            Some(line.substring(index, suffixIndex + suffixSize))
          } else {
            // No suffix so "clear" the current line
            currentLine = ""
            val value = line.substring(index)
            readUntilSuffixOrNone(value).flatMap {
              case SuffixReadResult(line, index, builder) =>
                currentLine = line.substring(index + suffixSize)
                builder.append(line.substring(0, index + suffixSize))
                Some(builder.toString())
            }
          }
      }

  def close(): Unit =
    input.close()

  private def readUntilPrefixOrNone(): Option[PrefixReadResult] = {
    //if the currentLine contains the suffix then return it
    val currentLineIndex = currentLine.indexOf(prefix)
    if (currentLineIndex >= 0) {
      Some(PrefixReadResult(currentLine, currentLineIndex))
    } else {
      var line  = br.readLine()
      var index = Option(line).map(_.indexOf(prefix)).getOrElse(-1)
      while (line != null && index < 0) {
        line  = br.readLine()
        index = Option(line).map(_.indexOf(prefix)).getOrElse(-1)
      }
      Option(line).map(l => PrefixReadResult(l, index))
    }
  }

  private def readUntilSuffixOrNone(initial: String): Option[SuffixReadResult] = {
    var line  = br.readLine()
    val sb    = new StringBuilder(initial)
    var index = Option(line).map(_.indexOf(suffix)).getOrElse(-1)
    while (line != null && index < 0) {
      sb.append(System.lineSeparator())
      sb.append(line)
      line  = br.readLine()
      index = Option(line).map(_.indexOf(suffix)).getOrElse(-1)
    }
    Option(line).map { l =>
      sb.append(System.lineSeparator())
      SuffixReadResult(l, index, sb)
    }
  }

  private case class PrefixReadResult(line: String, index: Int)
  private case class SuffixReadResult(line: String, index: Int, builder: StringBuilder)

}
