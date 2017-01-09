package com.datamountaineer.streamreactor.connect.ftp

import java.util

import org.apache.kafka.common.Configurable
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._

trait SourceRecordConverter extends Configurable {
  def convert(in:SourceRecord) : java.util.List[SourceRecord]
}

class NopSourceRecordConverter extends SourceRecordConverter{
  override def configure(props: util.Map[String, _]): Unit = {}

  override def convert(in: SourceRecord): util.List[SourceRecord] = Seq(in).asJava
}