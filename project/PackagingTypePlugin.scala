/*
 *  Copyright 2017-2018 Landoop LTD
 */

import sbt._

object PackagingTypePlugin extends AutoPlugin {
  override val buildSettings = {
    sys.props += "packaging.type" -> "jar"
    Nil
  }
}
