package com.datamountaineer.streamreactor.socketstreamer.avro

import kafka.serializer.Decoder

case object StringDecoder extends Decoder[AnyRef] {
  override def fromBytes(bytes: Array[Byte]): AnyRef = new String(bytes)
}
