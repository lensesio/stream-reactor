package com.datamountaineeer.streamreactor.connect.blockchain.data

case class BlockchainMessage(op: String, x: Option[Transaction], msg:Option[String])
