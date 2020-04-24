package com.landoop.streamreactor.connect.hive.sink.mapper

import cats.data.NonEmptyList
import com.datamountaineer.kcql.Field
import com.landoop.streamreactor.connect.hive.StructMapper
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}

/**
  * compile of [[StructMapper]] that will apply
  * a KCQL projection - dropping fields not specified in the
  * projection and mapping fields to their aliases.
  */
class ProjectionMapper(projection: NonEmptyList[Field]) extends StructMapper {

  override def map(input: Struct): Struct = {
    // the compatible output schema built from projected fields with aliases applied
    val builder = projection.foldLeft(SchemaBuilder.struct) { (builder, kcqlField) =>
      Option(input.schema.field(kcqlField.getName)).fold(sys.error(s"Missing field $kcqlField")) { field =>
        builder.field(kcqlField.getAlias, field.schema)
      }
    }
    val schema = builder.build()
    projection.foldLeft(new Struct(schema)) { (struct, field) =>
      struct.put(field.getAlias, input.get(field.getName))
    }
  }
}
