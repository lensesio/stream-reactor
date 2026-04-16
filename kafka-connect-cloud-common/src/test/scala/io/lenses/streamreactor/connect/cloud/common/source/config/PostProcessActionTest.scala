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
package io.lenses.streamreactor.connect.cloud.common.source.config

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionBucket
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionPrefix
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionRetain
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionWatermarkProcessLateArrival
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEntry
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum.Delete
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum.Move
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import org.apache.kafka.common.config.ConfigException
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PostProcessActionTest extends AnyFlatSpec with Matchers with EitherValues with OptionValues with MockitoSugar {

  "PostProcessAction.apply" should "return DeletePostProcessAction when Delete is specified" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Delete))
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false)).thenReturn(true.asRight)

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.value.value shouldBe a[DeletePostProcessAction]
  }

  it should "return MovePostProcessAction when Move is specified and bucket and prefix are provided" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false)).thenReturn(true.asRight)
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionWatermarkProcessLateArrival, default = false)).thenReturn(
      false.asRight,
    )
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(Some("some/prefix"))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(Some("myNewBucket"))

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.value.value shouldBe a[MovePostProcessAction]
    result.value.value.asInstanceOf[MovePostProcessAction].newPrefix shouldBe "some/prefix"
    result.value.value.asInstanceOf[MovePostProcessAction].newBucket shouldBe "myNewBucket"
    result.value.value.asInstanceOf[MovePostProcessAction].processLateArrival shouldBe false
  }

  it should "return MovePostProcessAction with processLateArrival=true when specified" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false)).thenReturn(false.asRight)
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionWatermarkProcessLateArrival, default = false)).thenReturn(
      true.asRight,
    )
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(Some("archive/prefix"))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(Some("archiveBucket"))

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.value.value shouldBe a[MovePostProcessAction]
    val moveAction = result.value.value.asInstanceOf[MovePostProcessAction]
    moveAction.processLateArrival shouldBe true
    moveAction.newPrefix shouldBe "archive/prefix"
    moveAction.newBucket shouldBe "archiveBucket"
  }

  it should "return an error when Move is specified but no prefix is provided" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(None)
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(Some("myNewBucket"))

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.left.value shouldBe an[IllegalArgumentException]
  }

  it should "return an error when Move is specified but no bucket is provided" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(Some("my/prefix"))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(None)

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.left.value shouldBe an[IllegalArgumentException]
  }

  it should "return None when no PostProcessAction is specified" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(None)

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.value shouldBe None
  }

  it should "drop the last character if it is a slash" in {
    PostProcessAction.dropEndSlash("some/prefix/") shouldBe "some/prefix"

    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false)).thenReturn(true.asRight)
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionWatermarkProcessLateArrival, default = false)).thenReturn(
      false.asRight,
    )
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(Some("some/prefix/"))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(Some("myNewBucket/"))

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.value.value shouldBe a[MovePostProcessAction]
    result.value.value.asInstanceOf[MovePostProcessAction].newPrefix shouldBe "some/prefix"
    result.value.value.asInstanceOf[MovePostProcessAction].newBucket shouldBe "myNewBucket"
  }

  it should "propagate error message when retainDirs has invalid boolean value" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(Some("myNewBucket"))
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(Some("some/prefix"))
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false))
      .thenReturn(
        new ConfigException(
          "Invalid value for configuration [post.process.action.retain.dirs]. The value must be one of: true, false.",
        ).asLeft,
      )

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.left.value shouldBe a[ConfigException]
    result.left.value.getMessage should include("post.process.action.retain.dirs")
    result.left.value.getMessage should include("true, false")
  }

  it should "propagate error message when processLateArrival has invalid boolean value" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(Some("myNewBucket"))
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(Some("some/prefix"))
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false)).thenReturn(false.asRight)
    when(kcqlProperties.getBooleanOrDefault(PostProcessActionWatermarkProcessLateArrival, default = false))
      .thenReturn(
        new ConfigException(
          "Invalid value for configuration [post.process.action.watermark.process.late.arrival]. The value must be one of: true, false.",
        ).asLeft,
      )

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.left.value shouldBe a[ConfigException]
    result.left.value.getMessage should include("post.process.action.watermark.process.late.arrival")
    result.left.value.getMessage should include("true, false")
  }

  it should "return specific error when bucket is missing" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(None)
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(Some("some/prefix"))

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.left.value shouldBe an[IllegalArgumentException]
    result.left.value.getMessage should include("bucket")
  }

  it should "return specific error when prefix is missing" in {
    val kcqlProperties = mock[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]]
    when(
      kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                      PropsKeyEnum.PostProcessAction,
      ),
    )
      .thenReturn(Some(Move))
    when(kcqlProperties.getString(PostProcessActionBucket)).thenReturn(Some("myNewBucket"))
    when(kcqlProperties.getString(PostProcessActionPrefix)).thenReturn(None)

    val result = PostProcessAction(Option.empty, kcqlProperties)

    result.left.value shouldBe an[IllegalArgumentException]
    result.left.value.getMessage should include("prefix")
  }

}
