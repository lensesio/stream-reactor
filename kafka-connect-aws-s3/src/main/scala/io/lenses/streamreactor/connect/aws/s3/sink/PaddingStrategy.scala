package io.lenses.streamreactor.connect.aws.s3.sink

trait PaddingStrategy {
  def padString(padMe: String): String
}

object NoOpPaddingStrategy extends PaddingStrategy {
  override def padString(dontPadMe: String): String = dontPadMe
}

case class LeftPadPaddingStrategy(maxDigits: Int, padCharacter: Char) extends PaddingStrategy {
  override def padString(padMe: String): String = padMe.reverse.padTo(maxDigits, padCharacter).reverse
}

case class RightPadPaddingStrategy(maxDigits: Int, padCharacter: Char) extends PaddingStrategy {
  override def padString(padMe: String): String = padMe.padTo(maxDigits, padCharacter)
}
