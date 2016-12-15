package com.datamountaineer.streamreactor.connect.druid.config

import java.nio.file.Paths
import com.datamountaineer.streamreactor.connect.druid.TestBase
import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import org.apache.kafka.common.config.ConfigException
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class DruidSinkSettingsTest extends WordSpec with TestBase with Matchers with MockitoSugar {
  "DruidSinkSettings" should {
    "raise an exception if the config file is not specified" in {
      intercept[ConfigException] {
        val config = new DruidSinkConfig(getPropsNoFile())
        DruidSinkSettings(config)
      }
    }
    "raise an exception if the config file specified doesn't exist" in {
      intercept[ConfigException] {
        val config = new DruidSinkConfig(getPropWrongPath())
        DruidSinkSettings(config)
      }
    }
    "create an instance of DruidSinkSettings with field alias" in {
      val config = mock[DruidSinkConfig]
      when(config.getString(DruidSinkConfig.KCQL)).thenReturn(KCQL)
      when(config.getString(DruidSinkConfig.CONFIG_FILE)).thenReturn(Paths.get(getClass.getResource(s"/ds-template.json").toURI).toAbsolutePath.toString)
      val settings = DruidSinkSettings(config)

      settings.datasourceNames shouldBe Map(TOPIC->DATA_SOURCE)
      settings.tranquilityConfig shouldBe scala.io.Source.fromFile(Paths.get(getClass.getResource(s"/ds-template.json").toURI).toFile).mkString

      val x: StructFieldsExtractor = settings.extractors.get(TOPIC).get
      x.includeAllFields shouldBe false
      x.fieldsAliasMap.get("page").get shouldBe "page"
      x.fieldsAliasMap.get("robot").get shouldBe "bot"
      x.fieldsAliasMap.get("country").get shouldBe "country"
    }

  }
}
