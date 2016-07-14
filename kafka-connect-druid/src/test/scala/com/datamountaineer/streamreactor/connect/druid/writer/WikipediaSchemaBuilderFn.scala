package com.datamountaineer.streamreactor.connect.druid.writer

object WikipediaSchemaBuilderFn {
  def apply(): Schema = {
    val schema = SchemaBuilder.struct().name("com.example.Wikipedia")
      .field("page", Schema.STRING_SCHEMA)
      .field("language", Schema.STRING_SCHEMA)
      .field("user", Schema.STRING_SCHEMA)
      .field("unpatrolled", Schema.BOOLEAN_SCHEMA)
      .field("newPage", Schema.BOOLEAN_SCHEMA)
      .field("robot", Schema.BOOLEAN_SCHEMA)
      .field("anonymous", Schema.BOOLEAN_SCHEMA)
      .field("namespace", Schema.STRING_SCHEMA)
      .field("continent", Schema.STRING_SCHEMA)
      .field("country", Schema.STRING_SCHEMA)
      .field("region", Schema.STRING_SCHEMA)
      .field("city", Schema.STRING_SCHEMA)
      .build()
    schema
  }
}
