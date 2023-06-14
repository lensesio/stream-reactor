package io.lenses.streamreactor.connect.aws.s3.sink.config

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.jdk.CollectionConverters.MapHasAsJava

class DeleteModeSettingTest extends AnyFlatSpec with Matchers with LazyLogging {
  it should "respect the delete mode setting" in {
    val deleteModeMap = Table[String, String, Boolean](
      ("testName", "value", "expected"),
      ("all-at-once", "AllAtOnce", true),
      ("individual", "Individual", false),
    )

    forAll(deleteModeMap) {
      (name: String, value: String, expected: Boolean) =>
        logger.debug("Executing {}", name)
        S3SinkConfigDefBuilder(Map(
          "connect.s3.kcql" -> "abc",
          "connect.s3.source.delete.mode" -> value,
        ).asJava).batchDelete() should be(expected)
    }
  }
}
