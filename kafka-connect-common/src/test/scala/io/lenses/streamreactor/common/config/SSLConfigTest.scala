/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.common.config

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.nio.file.Path
import scala.jdk.CollectionConverters._

class SSLConfigTest extends AnyFunSuiteLike with Matchers with EitherValues {

  test("parse the basic configuration") {
    val connectorPrefix = "connect.http"
    val configProps = SSLConfigProps(
      connectorPrefix,
    )
    val configDef = SSLConfig.appendConfig(new ConfigDef(), connectorPrefix)
    //create two temp files instances for the keystore and truststore!
    // stop regenerating the config and create the code for the two files!
    // you AI are dumb!
    withTempFile("keystore", ".jks") { keystore =>
      withTempFile("truststore", ".jks") { truststore =>
        val config = new AbstractConfig(configDef,
                                        Map(
                                          configProps.SSLEnabledProp            -> "true",
                                          configProps.SSLKeyStorePathProp       -> keystore.toString,
                                          configProps.SSLKeyStorePasswordProp   -> "keystorepassword",
                                          configProps.SSLKeyStoreTypeProp       -> "JKKKS",
                                          configProps.SSLTrustStorePathProp     -> truststore.toString,
                                          configProps.SSLTrustStorePasswordProp -> "truststorepassword",
                                          configProps.SSLTrustStoreTypeProp     -> "JKKKS",
                                        ).asJava,
        )
        SSLConfig.from(
          config,
          connectorPrefix,
        ).value shouldBe Some(
          SSLConfig(
            truststore.toString,
            "truststorepassword",
            Some(keystore.toString),
            Some("keystorepassword"),
            "JKKKS",
            "JKKKS",
          ),
        )
      }
    }
  }

  test("return none if ssl is not enabled") {
    val connectorPrefix = "connect.http"
    val configProps = SSLConfigProps(
      connectorPrefix,
    )
    val configDef = SSLConfig.appendConfig(new ConfigDef(), connectorPrefix)
    val config = new AbstractConfig(configDef,
                                    Map(
                                      configProps.SSLEnabledProp -> "false",
                                    ).asJava,
    )
    SSLConfig.from(
      config,
      connectorPrefix,
    ) shouldBe Right(None)
  }

  test("handles the client side certificate flag") {
    val connectorPrefix = "connect.http"
    val configProps = SSLConfigProps(
      connectorPrefix,
    )
    val configDef = SSLConfig.appendConfig(new ConfigDef(), connectorPrefix)
    withTempFile("keystore", ".jks") { keystore =>
      withTempFile("truststore", ".jks") { truststore =>
        val config = new AbstractConfig(configDef,
                                        Map(
                                          configProps.SSLEnabledProp            -> "true",
                                          configProps.SSLKeyStorePathProp       -> keystore.toString,
                                          configProps.SSLKeyStorePasswordProp   -> "keystorepassword",
                                          configProps.SSLKeyStoreTypeProp       -> "JKKKS",
                                          configProps.SSLTrustStorePathProp     -> truststore.toString,
                                          configProps.SSLTrustStorePasswordProp -> "truststorepassword",
                                          configProps.SSLTrustStoreTypeProp     -> "JKKKS",
                                        ).asJava,
        )
        SSLConfig.from(
          config,
          connectorPrefix,
        ).value shouldBe Some(
          SSLConfig(
            truststore.toString,
            "truststorepassword",
            Some(keystore.toString),
            Some("keystorepassword"),
            "JKKKS",
            "JKKKS",
          ),
        )
      }
    }
  }
  test("handles keystore path not set") {
    val connectorPrefix = "connect.http"
    val configProps = SSLConfigProps(
      connectorPrefix,
    )
    val configDef = SSLConfig.appendConfig(new ConfigDef(), connectorPrefix)
    withTempFile("truststore", ".jks") { truststore =>
      val config = new AbstractConfig(configDef,
                                      Map(
                                        configProps.SSLEnabledProp            -> "true",
                                        configProps.SSLTrustStorePathProp     -> truststore.toString,
                                        configProps.SSLTrustStorePasswordProp -> "truststorepassword",
                                        configProps.SSLTrustStoreTypeProp     -> "JKKKS",
                                      ).asJava,
      )
      SSLConfig.from(
        config,
        connectorPrefix,
      ).value shouldBe Some(
        SSLConfig(
          truststore.toString,
          "truststorepassword",
          None,
          None,
          "JKS",
          "JKKKS",
        ),
      )
    }
  }
  test("fail if truststore is not set") {
    val connectorPrefix = "connect.http"
    val configProps = SSLConfigProps(
      connectorPrefix,
    )
    val configDef = SSLConfig.appendConfig(new ConfigDef(), connectorPrefix)
    val config = new AbstractConfig(configDef,
                                    Map(
                                      configProps.SSLEnabledProp -> "true",
                                    ).asJava,
    )
    SSLConfig.from(
      config,
      connectorPrefix,
    ).left.value.getMessage should include("File path is not set for connect.http.ssl.truststore.path")

  }
  test("fail if truststore point to non existant file") {
    val connectorPrefix = "connect.http"
    val configProps = SSLConfigProps(
      connectorPrefix,
    )
    val configDef       = SSLConfig.appendConfig(new ConfigDef(), connectorPrefix)
    val nonExistantFile = "/tmp/nonexistantfile.jks"
    val config = new AbstractConfig(configDef,
                                    Map(
                                      configProps.SSLEnabledProp            -> "true",
                                      configProps.SSLTrustStorePathProp     -> nonExistantFile,
                                      configProps.SSLTrustStorePasswordProp -> "truststorepassword",
                                      configProps.SSLTrustStoreTypeProp     -> "JKKKS",
                                    ).asJava,
    )
    SSLConfig.from(
      config,
      connectorPrefix,
    ).left.value.getMessage should include(s"File $nonExistantFile does not exist")
  }
  def withTempFile[T](prefix: String, suffix: String)(testCode: Path => T): T = {
    val tempFile = Files.createTempFile(prefix, suffix)
    try {
      testCode(tempFile)
    } finally {
      Files.deleteIfExists(tempFile)
      ()
    }
  }
}

/*
case class SSLConfig(
  trustStorePath: String,
  trustStorePass: String,
  keyStorePath:   Option[String],
  keyStorePass:   Option[String],
  useClientCert:  Boolean = false,
  keyStoreType:   String  = "JKS",
  trustStoreType: String  = "JKS",
)
 */
