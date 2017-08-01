/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import java.util

import com.datamountaineer.streamreactor.connect.cassandra.cdc.ConnectState
import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CdcConfig
import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.{ChangeStructBuilder, SubscriptionDataProvider}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.cassandra.db.DeletionTime
import org.apache.cassandra.db.commitlog.CommitLogDescriptor
import org.apache.cassandra.db.partitions.PartitionUpdate
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConversions._

object PartitionUpdateExtensions extends StrictLogging {

  implicit class PartitionUpdateConverter(val partitionUpdate: PartitionUpdate) extends AnyVal {
    def read(keyspace: String,
             location: Int,
             desc: CommitLogDescriptor,
             callback: SourceRecord => Unit)(implicit dataProvider: SubscriptionDataProvider, cdcConfig: CdcConfig): Unit = {
      val cfMetadata = partitionUpdate.metadata()
      val topic = dataProvider
        .getTopic(cfMetadata.ksName, cfMetadata.cfName)
        .getOrElse(throw new IllegalArgumentException(s"There was a problem identifying the topic for '${cfMetadata.ksName}.${cfMetadata.cfName}'."))

      val key = KeyValueBuilder(cfMetadata, partitionUpdate)

      val offset = Offset(desc.fileName(), location, desc.id)
      val valueSchema = dataProvider.getStruct(cfMetadata.ksName, cfMetadata.cfName)
        .getOrElse(throw new IllegalArgumentException(s"There was a problem identifying the Connect Source Record schema for '${cfMetadata.ksName}.${cfMetadata.cfName}'."))

      //do we have a delete?
      if (!(partitionUpdate.deletionInfo().getPartitionDeletion eq DeletionTime.LIVE)) {
        //logger.debug(s"DELETE $keyspace.${cfMetadata.cfName} WHERE ${key.get(ChangeStructBuilder.KeysField).asInstanceOf[Struct].}")
        key.put(ChangeStructBuilder.ChangeTypeField, ChangeType.DELETE.toString)

        val sourceRecord = new SourceRecord(
          ConnectState.Key,
          offset.toMap(),
          topic,
          key.schema(),
          key,
          null,
          null
        )
        callback(sourceRecord)
        //}
      } else {
        partitionUpdate.foreach { row =>
          val value = new Struct(valueSchema)
          PopulatePKColumns(value, cfMetadata, partitionUpdate)
          PopulateClusteringColumns(value, cfMetadata, row.clustering())

          val deletedColumns = new util.ArrayList[String]()
          val hasTombstone = row.foldLeft(false) { case (b, cd) =>
            val (tombstoned, fieldValue) = PutColumnChange(valueSchema.field(cd.column().name.toString).schema(), cd, List(cd.column().name.toString), deletedColumns)

            value.put(cd.column().name.toString, fieldValue)
            b || tombstoned
          }

          //Unfortunately we can't work out an UPDATE from an INSERT;  Cassandra is not giving us that context

          //The UPDATE is available for DELETE column only
          //For some reason 'insert ... rlist' is giving us hasDeletion(nowInSec)=true; Don't understand why(bug in cassandra cdc?!)
          //so we need to check any cell have been tombstone-ed
          val changeType = if (hasTombstone) {
            //row.hasDeletion(nowInSec) && !(row.deletion().time() eq DeletionTime.LIVE)) {
            key.put(ChangeStructBuilder.ChangeTypeField, ChangeType.DELETE_COLUMN.toString)
            if (deletedColumns.nonEmpty) {
              key.put(ChangeStructBuilder.DeletedColumnsField, deletedColumns)
            }
          } else {
            key.put(ChangeStructBuilder.ChangeTypeField, ChangeType.INSERT.toString)
          }

          val sourceRecord = new SourceRecord(
            ConnectState.Key,
            offset.toMap(),
            topic,
            key.schema(),
            key,
            valueSchema,
            value
          )

          callback(sourceRecord)
        }
      }
    }

  }

}

