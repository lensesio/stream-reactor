package io.lenses.streamreactor.connect.io.text

trait LineReader extends AutoCloseable {
  def next(): Option[String]
}
