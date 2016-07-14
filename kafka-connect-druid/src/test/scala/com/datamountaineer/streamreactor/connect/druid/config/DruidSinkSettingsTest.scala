package com.datamountaineer.streamreactor.connect.druid.config

import java.nio.file.Paths

class DruidSinkSettingsTest extends WordSpec with Matchers with MockitoSugar {
  "DruidSinkSettings" should {
    "raise an exception if the config file is not specified" in {
      intercept[ConfigException] {
        val config = new DruidSinkConfig(Map(DruidSinkConfig.DATASOURCE_NAME -> "the_data_source"))
        DruidSinkSettings(config)
      }
    }
    "raise an exception if the config file specified doesn't exist" in {
      intercept[ConfigException] {
        val config = new DruidSinkConfig(
          Map(
            DruidSinkConfig.DATASOURCE_NAME -> "the_data_source",
            DruidSinkConfig.CONFIG_FILE -> "something.json"))
        DruidSinkSettings(config)
      }
    }
    "create an instance of DruidSinkSettings" in {
      val config = mock[DruidSinkConfig]
      when(config.getString(DruidSinkConfig.DATASOURCE_NAME)).thenReturn("wikipedia")
      when(config.getString(DruidSinkConfig.CONFIG_FILE)).thenReturn(Paths.get(getClass.getResource(s"/example.json").toURI).toAbsolutePath.toString)
      when(config.getString(DruidSinkConfig.FIELDS)).thenReturn("page=url,robot,country")
      val settings = DruidSinkSettings(config)

      settings.datasourceName shouldBe "wikipedia"
      settings.tranquilityConfig shouldBe scala.io.Source.fromFile(Paths.get(getClass.getResource(s"/example.json").toURI).toFile).mkString
      settings.payloadFields.includeAllFields shouldBe false
      settings.payloadFields.fieldsMappings.get("page").get shouldBe "url"
      settings.payloadFields.fieldsMappings.get("robot").get shouldBe "robot"
      settings.payloadFields.fieldsMappings.get("country").get shouldBe "country"
    }

  }
}
