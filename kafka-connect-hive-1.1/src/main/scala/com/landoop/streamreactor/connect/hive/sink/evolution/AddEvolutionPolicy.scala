package com.landoop.streamreactor.connect.hive.sink.evolution

import com.landoop.streamreactor.connect.hive.{DatabaseName, HiveSchemas, TableName}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Schema

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * An compile of [[EvolutionPolicy]] that attempts to evolve
  * the metastore schema to match the input schema by adding missing fields.
  */
object AddEvolutionPolicy extends EvolutionPolicy {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: Schema,
                      inputSchema: Schema)
                     (implicit client: IMetaStoreClient): Try[Schema] = Try {

    val missing = inputSchema.fields.asScala
      .filter(f => metastoreSchema.field(f.name) == null)
      .map(HiveSchemas.toFieldSchema)

    if (missing.nonEmpty) {
      logger.info(s"Evolving hive metastore to add: ${missing.mkString(",")}")

      val table = client.getTable(dbName.value, tableName.value)
      val cols = table.getSd.getCols
      missing.foreach(field => cols.add(field))
      table.getSd.setCols(cols)
      client.alter_table(dbName.value, tableName.value, table)

      HiveSchemas.toKafka(client.getTable(dbName.value, tableName.value))

    } else {
      metastoreSchema
    }
  }
}
