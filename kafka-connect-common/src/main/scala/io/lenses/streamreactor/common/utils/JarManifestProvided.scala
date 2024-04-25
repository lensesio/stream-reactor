package io.lenses.streamreactor.common.utils

import io.lenses.streamreactor.common.util.JarManifest

trait JarManifestProvided {

  lazy val manifest: JarManifest = new JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  def version(): String = manifest.getVersion()

}
