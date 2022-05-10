package com.datamountaineer.streamreactor.connect.jms.config

import com.datamountaineer.streamreactor.connect.jms.sink.converters.JMSSinkMessageConverter
import com.datamountaineer.streamreactor.connect.jms.source.converters.JMSSourceMessageConverter
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class SourceConverterConfigWrapperTest extends AnyWordSpecLike with MockitoSugar with EitherValues with Matchers {
  private val converter = mock[JMSSourceMessageConverter]
  private val wrapper = SourceConverterConfigWrapper(converter)

  "should return the converter when requesting converter for source" in {
    wrapper.forSource.value shouldBe (converter)
  }

  "should return an error when requesting converter for sink" in {
    wrapper.forSink.left.value should be ("Configured source, requested sink")
  }
}
class SinkConverterConfigWrapperTest extends AnyWordSpecLike with MockitoSugar with EitherValues with Matchers {
  private val converter = mock[JMSSinkMessageConverter]
  private val wrapper = SinkConverterConfigWrapper(converter)

  "should return the converter when requesting converter for sink" in {
    wrapper.forSink.value shouldBe (converter)
  }

  "should return an error when requesting converter for source" in {
    wrapper.forSource.left.value should be ("Configured sink, requested source")
  }
}
