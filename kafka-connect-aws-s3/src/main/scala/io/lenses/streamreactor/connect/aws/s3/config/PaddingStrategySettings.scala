package io.lenses.streamreactor.connect.aws.s3.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.PADDING_LENGTH
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.PADDING_STRATEGY
import io.lenses.streamreactor.connect.aws.s3.sink.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.NoOpPaddingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.PaddingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.RightPadPaddingStrategy

sealed trait PaddingStrategyOptions extends EnumEntry

object PaddingStrategyOptions extends Enum[PaddingStrategyOptions] {

  val values = findValues

  case object LeftPad  extends PaddingStrategyOptions
  case object RightPad extends PaddingStrategyOptions
  case object NoOp     extends PaddingStrategyOptions

}

trait PaddingStrategySettings extends BaseSettings {

  private val paddingChar: Char = '0'

  def getPaddingStrategy(): PaddingStrategy = {
    val paddingLength = getInt(PADDING_LENGTH)
    PaddingStrategyOptions.withNameInsensitive(getString(PADDING_STRATEGY)) match {
      case PaddingStrategyOptions.LeftPad  => LeftPadPaddingStrategy(paddingLength, paddingChar)
      case PaddingStrategyOptions.RightPad => RightPadPaddingStrategy(paddingLength, paddingChar)
      case _                               => NoOpPaddingStrategy
    }
  }
}
