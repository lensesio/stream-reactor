package com.landoop.streamreactor.connect.hive.sink.evolution

import com.landoop.streamreactor.connect.hive.{DatabaseName, HiveSchemas, TableName}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Schema

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * An implementation of [[EvolutionPolicy]] that requires the
  * input schema to be equal to the metastore schema.
  *
  * This means that every field in the metastore schema must be
  * present in the incoming records, and the incoming records
  * cannot contain any extra fields.
  */
object StrictEvolutionPolicy extends EvolutionPolicy {

  override def evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: Schema,
                      inputSchema: Schema)
                     (implicit client: IMetaStoreClient): Try[Schema] = Try {
    HiveSchemas.toKafka(client.getTable(dbName.value, tableName.value))
  }.map { schema =>
    val compatible = schema.fields().asScala.forall { field =>
      inputSchema.field(field.name) != null ||
        field.schema().isOptional ||
        field.schema().defaultValue() != null
    }
    if (compatible) schema else sys.error("Input Schema is not compatible with the metastore")
  }
}
