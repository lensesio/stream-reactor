package com.landoop.streamreactor.connect.hive.sink.evolution

import com.landoop.streamreactor.connect.hive.{DatabaseName, TableName}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Schema

import scala.util.{Failure, Try}

/**
  * When preparing to write data to hive, the incoming records may have a
  * different schema to that which is present in the hive metastore.
  *
  * Sometimes, it is possible to evolve the hive metastore schema so that
  * it becomes compatible with the incoming records. Other times this is
  * not possible or may not always be desirable even if technically possible.
  *
  * Some file formats are tabular, and do not include field names (for example
  * delimited data). When writing in such a format the ordering of the columns
  * is important, as hive has to rely on the metastore schema in order
  * to align columns with values.
  *
  * Therefore to successfully evolve the metastore schema, the following steps
  * must be followed.
  *
  * 1. New fields can be added if a suitable default (or null) is provided,
  * so that files written before the evolution can still be read.
  *
  * 2. For a file format without field names, any new columns must be added
  * at the end of the list of columns.
  *
  * 3. Fields can be removed only if the table uses a file format with an
  * embedded schema.
  *
  * 4. Data types can be changed only if the new datatype allows conversion from
  * the old without data loss. Eg, an Int field can be evolved into a Long,
  * but a String cannot be evolved into a Boolean.
  */
trait EvolutionPolicy {

  /**
    * Accepts an input [[Schema]], which is the schema defined by a record.
    *
    * This method should return the metastore schema after the result of
    * evolution, or in the case of no changes, the original metastore schema.
    *
    * If the metastore schema is not compatible with the input schema
    * and it cannot or should not be evolved, then this method should
    * return a [[Failure]].
    */
  def evolve(dbName: DatabaseName,
             tableName: TableName,
             metastoreSchema: Schema,
             inputSchema: Schema)(implicit client: IMetaStoreClient): Try[Schema]
}