package com.landoop.streamreactor.connect.hive.sink.mapper

import com.landoop.streamreactor.connect.hive.StructMapper
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.util.Try

/**
 * An compile of [[StructMapper]] that will align an input
 * record so that it's structure matches that defined in the metastore.
 *
 * Sometimes an input record will not contain a value for a field defined
 * by the metastore schema. If the metastore field has a default value
 * or allows nulls, then output record could be padded to include that field.
 *
 * Secondly, the input record may specify extra fields which are not required
 * by the hive table. Therefore these records can be safely dropped.
 *
 * Lastly, for file formats that do not include field information, such as
 * CSV without headers, the field orderings must match the hive metastore.
 * An input record may include all the required fields but in a different
 * order, and so this transformer can reorder the fields as required.
 */
class MetastoreSchemaAlignMapper(schema: Schema) extends StructMapper {

  import scala.collection.JavaConverters._

  override def map(input: Struct): Struct = {
    //hive converts everything to lowercase
    val inputFieldsMapping = input.schema().fields().asScala.map { f => f.name().toLowerCase() -> f.name() }.toMap
    val struct = schema.fields.asScala.foldLeft(new Struct(schema)) { (struct, field) =>
      Try(input.get(inputFieldsMapping(field.name))).toOption match {
        case Some(value) => struct.put(field.name, value)
        case None if field.schema.isOptional => struct.put(field.name, null)
        case None => sys.error(s"Cannot map struct to required schema; ${field.name} is missing, no default value has been supplied and null is not permitted")
      }
    }
    struct
  }
}
