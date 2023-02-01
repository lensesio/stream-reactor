package io.lenses.streamreactor.connect

import com.github.luben.zstd.NoPool
import com.hadoop.compression.lzo.LzopCodec
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.compress.CompressionCodec
import org.tukaani.xz.FilterOptions

/**
  * We don't ship every single compression codec jar with the connector as that would make it bloated.  For running the tests for certain codecs we can make the additional jars available to the container.
  */
object ProvidedJars extends LazyLogging {

  private val classesWithinJarsToProvide = Seq(
    classOf[FilterOptions],
    classOf[LzopCodec],
    classOf[NoPool],
    classOf[CompressionCodec],
  )

  val providedJars: Seq[String] = {
    val jars = classesWithinJarsToProvide.map(_.getProtectionDomain.getCodeSource.getLocation.getPath)
    logger.info("Jars to provide: {}", jars)
    jars
  }
}
