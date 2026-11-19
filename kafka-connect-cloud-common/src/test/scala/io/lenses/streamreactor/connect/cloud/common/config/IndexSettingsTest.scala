/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.config

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

class IndexSettingsTest extends AnyFunSuite with Matchers with OptionValues {

  import IndexSettingsFixture._

  test("[B] an absent commit.mode key yields CommitMode.Granular") {
    settingsFor(Map.empty).getIndexSettings.value.commitMode shouldBe CommitMode.Granular
  }

  test("[B] commit.mode=batch yields CommitMode.Batch") {
    settingsFor(Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> "batch"))
      .getIndexSettings.value.commitMode shouldBe CommitMode.Batch
  }

  test("[B] an unrecognised commit.mode is rejected by ConfigDef validation") {
    val ex = intercept[ConfigException] {
      new AbstractConfig(configDef, Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> "bogus").asJava)
    }
    ex.getMessage should include("bogus")
  }

  // ── case-insensitive commit.mode ────────────────────────────────────────────────────

  test("[B] commit.mode is accepted case-insensitively at the ConfigDef level") {
    Seq("batch", "Batch", "BATCH").foreach { value =>
      settingsFor(Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> value))
        .getIndexSettings.value.commitMode shouldBe CommitMode.Batch
    }
    Seq("granular", "Granular", "GRANULAR").foreach { value =>
      settingsFor(Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> value))
        .getIndexSettings.value.commitMode shouldBe CommitMode.Granular
    }
  }

  // ── commit.mode ignored when exactly-once disabled ──────────────────────────────────

  test("[B] commit.mode=batch has no effect (indexOptions is None) when exactly-once is disabled") {
    settingsFor(
      Map(
        Keys.ENABLE_EXACTLY_ONCE      -> "false",
        Keys.EXACTLY_ONCE_COMMIT_MODE -> "batch",
      ),
    ).getIndexSettings shouldBe None
  }

  // ── gc.sweep.max.reads below the sweep's minimum ────────────────────────────────────

  /** The WARN messages `settings` logs while `f` runs. */
  private def warningsDuring[A](settings: IndexSettings)(f: => A): (A, List[String]) = {
    // Resolved through the logback context under `LoggerFactory`'s monitor: while another suite is
    // still initialising SLF4J, `getLogger` hands out a substitute logger that would capture nothing.
    val context =
      classOf[LoggerFactory].synchronized(LoggerFactory.getILoggerFactory).asInstanceOf[LoggerContext]
    val logger   = context.getLogger(settings.getClass.getName)
    val appender = new ListAppender[ILoggingEvent]()
    appender.start()
    logger.addAppender(appender)
    try {
      val a = f
      (a, appender.list.asScala.toList.filter(_.getLevel == Level.WARN).map(_.getFormattedMessage))
    } finally {
      val _ = logger.detachAppender(appender)
    }
  }

  test("gc.sweep.max.reads=1 still validates, is used as 2, and is warned about once with both values") {
    val settings = settingsFor(Map(Keys.GC_SWEEP_MAX_READS -> "1"))

    val (budgets, warnings) = warningsDuring(settings) {
      // A task reads its index settings more than once while it builds its config.
      (1 to 3).map(_ => settings.getIndexSettings.value.gcSweepMaxReads)
    }

    budgets shouldBe Seq(2, 2, 2)
    warnings shouldBe List(
      "connect.testcloud.indexes.gc.sweep.max.reads=1 is below the minimum of 2 the granular lock orphan sweep " +
        "needs: each pass spends one read re-checking that this task still owns the partition's master lock " +
        "before it deletes anything, and needs at least one more to read a lock file. Using 2. Set it to 2 or " +
        "more to remove this warning.",
    )
  }

  test("gc.sweep.max.reads of 2 or more is used as configured, with no warning") {
    Seq("2", "3", "1000").foreach { value =>
      val settings           = settingsFor(Map(Keys.GC_SWEEP_MAX_READS -> value))
      val (budget, warnings) = warningsDuring(settings)(settings.getIndexSettings.value.gcSweepMaxReads)
      budget shouldBe value.toInt
      warnings shouldBe empty
    }
  }

  test("gc.sweep.max.reads=1 is not warned about when the granular lock orphan sweep cannot run") {
    Seq(
      Map(Keys.GC_SWEEP_MAX_READS -> "1", Keys.GC_SWEEP_ENABLED         -> "false"),
      Map(Keys.GC_SWEEP_MAX_READS -> "1", Keys.EXACTLY_ONCE_COMMIT_MODE -> "batch"),
    ).foreach { props =>
      val settings           = settingsFor(props)
      val (budget, warnings) = warningsDuring(settings)(settings.getIndexSettings.value.gcSweepMaxReads)
      budget shouldBe 2
      warnings shouldBe empty
    }
  }
}
