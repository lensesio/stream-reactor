package io.lenses.streamreactor.connect.aws.s3.formats

trait Using {

  def using[A, B <: {def close(): Unit}](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    } finally {
      closeable.close()
    }
}
