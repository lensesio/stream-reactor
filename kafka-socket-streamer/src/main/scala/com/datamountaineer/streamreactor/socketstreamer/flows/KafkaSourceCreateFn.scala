package com.datamountaineer.streamreactor.socketstreamer.flows

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import com.datamountaineer.streamreactor.socketstreamer.SocketStreamerConfig
import com.datamountaineer.streamreactor.socketstreamer.domain.KafkaClientProps
import com.datamountaineer.streamreactor.socketstreamer.flows.SourceExtension._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

object KafkaSourceCreateFn extends StrictLogging {

  /**
    * Create a Kafka source
    *
    * @param kafkaRequestProps A KafkaRequestProps to use to create a KafkaConsumer
    * @return A Source of [ConsumerRecord, Unit]
    **/
  def apply(kafkaRequestProps: KafkaClientProps)
           (implicit actorSystem: ActorSystem, config: SocketStreamerConfig): Source[ConsumerRecord[Array[Byte], Array[Byte]], Control] = {
    logger.info(s"Setting up Kafka consumer properties for topic ${kafkaRequestProps.topic}")

    val consumerSettings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(config.kafkaBrokers)
      .withGroupId(kafkaRequestProps.consumerGroup)
      //if an offset is out of range or the offset doesn't exist yet default to earliest available
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val kafkaClientSettings = (config.consumerProperties - ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      .foldLeft(consumerSettings) { case (settings, (k, v)) => settings.withProperty(k, v) }

    val source = if (kafkaRequestProps.parititionAndOffset.isEmpty) {
      Consumer.plainSource(kafkaClientSettings, Subscriptions.topics(kafkaRequestProps.topic))
    } else {
      buildMergedSources(kafkaClientSettings, kafkaRequestProps)
    }

    kafkaRequestProps.sample.fold(source) { p => source.withSampling(p.count, p.rate) }
  }

  def buildMergedSources(consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]], kafkaRequestProps: KafkaClientProps) = {
    //we could have scenarios where the user selects some partitions from a given offset whereas for the others they don't
    val (withOffset, withoutOffset) = kafkaRequestProps.parititionAndOffset.span(_.offset.isDefined)

    val sourceWithOffset = withOffset.headOption
      .map { _ =>
        val offsetsMap = withOffset.map { tp =>
          new TopicPartition(kafkaRequestProps.topic, tp.partition) -> tp.offset.get
        }.toMap

        Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(offsetsMap))
      }

    val sourceWithoutOffset = withoutOffset.headOption
      .map { _ =>
        val assignments = withoutOffset.map { tp => new TopicPartition(kafkaRequestProps.topic, tp.partition) }
        Consumer.plainSource(consumerSettings, Subscriptions.assignment(assignments: _*))
      }

    Seq(sourceWithOffset, sourceWithoutOffset).flatten match {
      case Seq(source1, source2) => source1.merge(source2)
      case Seq(source) => source
    }
  }

}
