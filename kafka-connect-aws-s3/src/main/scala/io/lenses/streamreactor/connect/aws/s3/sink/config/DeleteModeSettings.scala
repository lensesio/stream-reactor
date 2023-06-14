package io.lenses.streamreactor.connect.aws.s3.sink.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import enumeratum.EnumEntry
import enumeratum.Enum
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_DELETE_MODE

sealed trait DeleteModeOptions extends EnumEntry

object DeleteModeOptions extends Enum[DeleteModeOptions] {

  val values = findValues

  case object AllAtOnce  extends DeleteModeOptions
  case object Individual extends DeleteModeOptions
}

trait DeleteModeSettings extends BaseSettings {
  def batchDelete(): Boolean =
    DeleteModeOptions.withNameInsensitive(getString(SOURCE_DELETE_MODE)) match {
      case DeleteModeOptions.AllAtOnce  => true
      case DeleteModeOptions.Individual => false
    }
}
