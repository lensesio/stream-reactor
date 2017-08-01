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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, RemoteEndpointAwareJdkSSLOptions, Row, TypeCodec}
import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CassandraConfig
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.cassandra.config.ColumnDefinition.ClusteringOrder
import org.apache.cassandra.config._
import org.apache.cassandra.cql3.functions.{FunctionName, UDAggregate, UDFunction}
import org.apache.cassandra.cql3.statements.SelectStatement
import org.apache.cassandra.cql3.{ColumnIdentifier, QueryProcessor, Terms}
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.db.view.View
import org.apache.cassandra.exceptions.InvalidRequestException
import org.apache.cassandra.schema.CQLTypeParser.parse
import org.apache.cassandra.schema.{SchemaKeyspace, _}

import scala.collection.JavaConversions._

/**
  * The Cassandra CommitLogReader API works with its own Keyspace and CF metadata which are not the same class objects from the driver.
  * We need to connect to Cassandra and read the metadata otherwise an embedded Cassandra instance is required which is a no-go
  *
  */
class CassandraMetadataProvider(config: CassandraConfig) extends AutoCloseable with StrictLogging {
  require(
    config.contactPoints != null && config.contactPoints.trim.nonEmpty,
    s"Invalid contact points provided:'${config.contactPoints}'"
  )

  private val cluster = {
    val builder: Builder = Cluster
      .builder()
      .addContactPoints(config.contactPoints.split(","): _*)
      .withPort(config.port)

    builder.getConfiguration.getCodecRegistry
      .register(TypeCodec.map(TypeCodec.ascii(), TypeCodec.ascii()))
      .register(TypeCodec.map(TypeCodec.varchar(), TypeCodec.varchar()))
    for {u <- config.user
         p <- config.password} {
      builder.withCredentials(u.trim, p.trim)
    }

    config.ssl.foreach { ssl =>
      //val cipherSuites: Array[String] = Array("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
      val sslOptions = new RemoteEndpointAwareJdkSSLOptions.Builder().withSSLContext(ssl).build()
      //sslOptions.withCipherSuites(cipherSuites)
      builder.withSSL(sslOptions)
    }

    builder.build()
  }

  private val session = cluster.connect()

  def getKeyspaces(keyspaceNames: util.List[String]): Keyspaces = {
    val query = s"SELECT keyspace_name FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.KEYSPACES} WHERE keyspace_name IN ?"
    val keyspaces: Keyspaces.Builder = org.apache.cassandra.schema.Keyspaces.builder

    for (row <- executeQuery(query, keyspaceNames)) {
      val keyspaceName: String = row.getString("keyspace_name")
      if (keyspaceName.contains(keyspaceName)) {
        keyspaces.add(getKeyspace(keyspaceName))
      }
    }
    keyspaces.build
  }

  def getKeyspace(keyspaceName: String): KeyspaceMetadata = {
    val params = fetchKeyspaceParams(keyspaceName)
    val types = fetchTypes(keyspaceName)
    val tables = fetchTables(keyspaceName, types)
    val views = fetchViews(keyspaceName, types)
    val functions = fetchFunctions(keyspaceName, types)
    KeyspaceMetadata.create(keyspaceName, params, tables, views, types, functions)
  }

  def fetchColumnFamilyMetadata(keyspaceName: String, tableName: String): CFMetaData = {
    val params = fetchKeyspaceParams(keyspaceName)
    val types = fetchTypes(keyspaceName)
    fetchTable(keyspaceName, tableName, types)
  }

  private def fetchKeyspaceParams(keyspaceName: String): KeyspaceParams = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.KEYSPACES} WHERE keyspace_name = ?"

    val row: Row = executeQuery(query, keyspaceName).one
    val durableWrites: Boolean = row.getBool(KeyspaceParams.Option.DURABLE_WRITES.toString)
    val replication: util.Map[String, String] = getFrozenTextMap(row, KeyspaceParams.Option.REPLICATION.toString)
    KeyspaceParams.create(durableWrites, replication)

  }

  private def fetchTypes(keyspaceName: String) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.TYPES} WHERE keyspace_name = ?"
    val types = org.apache.cassandra.schema.Types.rawBuilder(keyspaceName)
    for (row <- executeQuery(query, keyspaceName)) {
      val name = row.getString("type_name")
      val fieldNames = getFrozenList(row, "field_names", UTF8Type.instance)
      val fieldTypes = getFrozenList(row, "field_types", UTF8Type.instance)
      types.add(name, fieldNames, fieldTypes)
    }
    types.build
  }

  private def fetchTables(keyspaceName: String, types: Types) = {
    val query = s"SELECT table_name FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.TABLES} WHERE keyspace_name = ?"
    val tables = org.apache.cassandra.schema.Tables.builder
    for (row <- executeQuery(query, keyspaceName)) {
      tables.add(fetchTable(keyspaceName, row.getString("table_name"), types))
    }
    tables.build
  }


  private def fetchTable(keyspaceName: String, tableName: String, types: Types): CFMetaData = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.TABLES} WHERE keyspace_name = ? AND table_name = ?"
    val rows = executeQuery(query, keyspaceName, tableName)
    if (rows.isEmpty) throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspaceName, tableName))
    val row = rows.one
    val id = row.getUUID("id")
    val flags = CFMetaData.flagsFromStrings(getFrozenSet(row, "flags", UTF8Type.instance))
    val isSuper = flags.contains(CFMetaData.Flag.SUPER)
    val isCounter = flags.contains(CFMetaData.Flag.COUNTER)
    val isDense = flags.contains(CFMetaData.Flag.DENSE)
    val isCompound = flags.contains(CFMetaData.Flag.COMPOUND)
    val columns = fetchColumns(keyspaceName, tableName, types)
    if (!columns.exists(_.isPartitionKey)) {
      val msg = s"Table $keyspaceName.$tableName did not have any partition key columns in the schema tables"
      throw new AssertionError(msg)
    }
    val droppedColumns = fetchDroppedColumns(keyspaceName, tableName)
    val indexes = fetchIndexes(keyspaceName, tableName)
    val triggers = fetchTriggers(keyspaceName, tableName)
    CFMetaData.create(keyspaceName, tableName, id, isDense, isCompound, isSuper, isCounter, false, columns, DatabaseDescriptor.getPartitioner)
      .params(createTableParamsFromRow(row))
      .droppedColumns(droppedColumns)
      .indexes(indexes)
      .triggers(triggers)
  }

  private def fetchColumns(keyspace: String, table: String, types: Types) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.COLUMNS} WHERE keyspace_name = ? AND table_name = ?"
    val columns = new util.ArrayList[ColumnDefinition]
    executeQuery(query, keyspace, table).foreach(row => columns.add(createColumnFromRow(row, types)))
    columns
  }

  private def createColumnFromRow(row: Row, types: Types): ColumnDefinition = {
    val keyspace = row.getString("keyspace_name")
    val table = row.getString("table_name")
    val kind = ColumnDefinition.Kind.valueOf(row.getString("kind").toUpperCase)
    val position = row.getInt("position")
    val order = ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase)
    var `type` = parse(keyspace, row.getString("type"), types)
    if (order eq ClusteringOrder.DESC) `type` = ReversedType.getInstance(`type`)
    val name = ColumnIdentifier.getInterned(`type`, row.getBytes("column_name_bytes"), row.getString("column_name"))
    new ColumnDefinition(keyspace, table, name, `type`, position, kind)
  }

  private def fetchDroppedColumns(keyspace: String, table: String) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.DROPPED_COLUMNS} WHERE keyspace_name = ? AND table_name = ?"
    val columns = new util.HashMap[ByteBuffer, CFMetaData.DroppedColumn]
    for (row <- executeQuery(query, keyspace, table)) {
      val column = createDroppedColumnFromRow(row)
      columns.put(UTF8Type.instance.decompose(column.name), column)
    }
    columns
  }

  private def createDroppedColumnFromRow(row: Row) = {
    val keyspace = row.getString("keyspace_name")
    val name = row.getString("column_name")
    /*
             * we never store actual UDT names in dropped column types (so that we can safely drop types if nothing refers to
             * them anymore), so before storing dropped columns in schema we expand UDTs to tuples. See expandUserTypes method.
             * Because of that, we can safely pass Types.none() to parse()
             */ val `type` = parse(keyspace, row.getString("type"), org.apache.cassandra.schema.Types.none)
    val droppedTime = TimeUnit.MILLISECONDS.toMicros(row.getLong("dropped_time"))
    new CFMetaData.DroppedColumn(name, `type`, droppedTime)
  }

  private def fetchIndexes(keyspace: String, table: String) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.INDEXES} WHERE keyspace_name = ? AND table_name = ?"
    val indexes = org.apache.cassandra.schema.Indexes.builder
    executeQuery(query, keyspace, table).foreach(row => indexes.add(createIndexMetadataFromRow(row)))
    indexes.build
  }

  private def createIndexMetadataFromRow(row: Row) = {
    val name = row.getString("index_name")
    val `type` = IndexMetadata.Kind.valueOf(row.getString("kind"))
    val options = getFrozenTextMap(row, "options")
    IndexMetadata.fromSchemaMetadata(name, `type`, options)
  }

  private def fetchTriggers(keyspace: String, table: String) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.TRIGGERS} WHERE keyspace_name = ? AND table_name = ?"
    val triggers = org.apache.cassandra.schema.Triggers.builder
    executeQuery(query, keyspace, table).foreach(row => triggers.add(createTriggerFromRow(row)))
    triggers.build
  }

  private def createTriggerFromRow(row: Row) = {
    val name = row.getString("trigger_name")
    val classOption = getFrozenTextMap(row, "options").get("class")
    new TriggerMetadata(name, classOption)
  }

  def createTableParamsFromRow(row: Row): TableParams = {
    TableParams.builder
      .bloomFilterFpChance(row.getDouble("bloom_filter_fp_chance"))
      .caching(CachingParams.fromMap(getFrozenTextMap(row, "caching")))
      .comment(row.getString("comment"))
      .compaction(CompactionParams.fromMap(getFrozenTextMap(row, "compaction")))
      .compression(CompressionParams.fromMap(getFrozenTextMap(row, "compression")))
      .dcLocalReadRepairChance(row.getDouble("dclocal_read_repair_chance"))
      .defaultTimeToLive(row.getInt("default_time_to_live"))
      .extensions(getFrozenMap(row, "extensions", UTF8Type.instance, BytesType.instance))
      .gcGraceSeconds(row.getInt("gc_grace_seconds"))
      .maxIndexInterval(row.getInt("max_index_interval"))
      .memtableFlushPeriodInMs(row.getInt("memtable_flush_period_in_ms"))
      .minIndexInterval(row.getInt("min_index_interval"))
      .readRepairChance(row.getDouble("read_repair_chance"))
      .crcCheckChance(row.getDouble("crc_check_chance"))
      .speculativeRetry(SpeculativeRetryParam.fromString(row.getString("speculative_retry")))
      .cdc(if (row.getColumnDefinitions.contains("cdc")) row.getBool("cdc") else false)
      .build
  }


  private def fetchViews(keyspaceName: String, types: Types) = {
    val query = s"SELECT view_name FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.VIEWS} WHERE keyspace_name = ?"
    val views = org.apache.cassandra.schema.Views.builder
    for (row <- executeQuery(query, keyspaceName)) {
      views.add(fetchView(keyspaceName, row.getString("view_name"), types))
    }
    views.build
  }

  private def fetchView(keyspaceName: String, viewName: String, types: Types) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.VIEWS} WHERE keyspace_name = ? AND view_name = ?"
    val rows = executeQuery(query, keyspaceName, viewName)
    if (rows.isEmpty) throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspaceName, viewName))
    val row = rows.one
    val id = row.getUUID("id")
    val baseTableId = row.getUUID("base_table_id")
    val baseTableName = row.getString("base_table_name")
    val includeAll = row.getBool("include_all_columns")
    val whereClause = row.getString("where_clause")
    val columns = fetchColumns(keyspaceName, viewName, types)
    val droppedColumns = fetchDroppedColumns(keyspaceName, viewName)
    val cfm = CFMetaData.create(keyspaceName, viewName, id, false, true, false, false, true, columns, DatabaseDescriptor.getPartitioner).params(createTableParamsFromRow(row)).droppedColumns(droppedColumns)
    val rawSelect = View.buildSelectStatement(baseTableName, columns, whereClause)
    val rawStatement = QueryProcessor.parseStatement(rawSelect).asInstanceOf[SelectStatement.RawStatement]
    new ViewDefinition(keyspaceName, viewName, baseTableId, baseTableName, includeAll, rawStatement, whereClause, cfm)
  }

  private def fetchFunctions(keyspaceName: String, types: Types) = {
    val udfs = fetchUDFs(keyspaceName, types)
    val udas = fetchUDAs(keyspaceName, udfs, types)
    org.apache.cassandra.schema.Functions.builder.add(udfs).add(udas).build
  }

  private def fetchUDFs(keyspaceName: String, types: Types) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.FUNCTIONS} WHERE keyspace_name = ?"
    val functions = org.apache.cassandra.schema.Functions.builder
    for (row <- executeQuery(query, keyspaceName)) {
      functions.add(createUDFFromRow(row, types))
    }
    functions.build
  }

  private def createUDFFromRow(row: Row, types: Types): UDFunction = {
    val ksName = row.getString("keyspace_name")
    val functionName = row.getString("function_name")
    val name = new FunctionName(ksName, functionName)
    val argNames = new util.ArrayList[ColumnIdentifier]
    import scala.collection.JavaConversions._
    for (arg <- getFrozenList(row, "argument_names", UTF8Type.instance)) {
      argNames.add(new ColumnIdentifier(arg, true))
    }
    val argTypes = new util.ArrayList[AbstractType[_]]
    for (t <- getFrozenList(row, "argument_types", UTF8Type.instance)) {
      argTypes.add(parse(ksName, t, types))
    }
    val returnType = parse(ksName, row.getString("return_type"), types)
    val language = row.getString("language")
    val body = row.getString("body")
    val calledOnNullInput = row.getBool("called_on_null_input")
    val existing = Schema.instance.findFunction(name, argTypes).orElse(null)
    existing match {
      case udf: UDFunction
        if udf.argNames == argNames && // arg types checked in Functions.find call
          udf.returnType == returnType && !udf.isAggregate && udf.language == language && udf.body == body && udf.isCalledOnNullInput == calledOnNullInput =>

        udf
      case _ =>
        try
          UDFunction.create(name, argNames, argTypes, returnType, calledOnNullInput, language, body)
        catch {
          case e: InvalidRequestException =>
            logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)", name), e)
            UDFunction.createBrokenFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, e)
        }
    }
  }

  private def fetchUDAs(keyspaceName: String, udfs: Functions, types: Types) = {
    val query = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.AGGREGATES} WHERE keyspace_name = ?"
    val aggregates = org.apache.cassandra.schema.Functions.builder
    for (row <- executeQuery(query, keyspaceName)) {
      aggregates.add(createUDAFromRow(row, udfs, types))
    }
    aggregates.build
  }

  private def createUDAFromRow(row: Row, functions: Functions, types: Types) = {
    val ksName = row.getString("keyspace_name")
    val functionName = row.getString("aggregate_name")
    val name = new FunctionName(ksName, functionName)
    val argTypes = getFrozenList(row, "argument_types", UTF8Type.instance).map(t => parse(ksName, t, types)).toList
    val returnType = parse(ksName, row.getString("return_type"), types)
    val stateFunc = new FunctionName(ksName, row.getString("state_func"))
    val finalFunc = if (row.getColumnDefinitions.contains("final_func")) new FunctionName(ksName, row.getString("final_func"))
    else null
    val stateType = if (row.getColumnDefinitions.contains("state_type")) parse(ksName, row.getString("state_type"), types)
    else null
    val initcond = if (row.getColumnDefinitions.contains("initcond")) Terms.asBytes(ksName, row.getString("initcond"), stateType)
    else null
    try
      UDAggregate.create(functions, name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond)
    catch {
      case reason: InvalidRequestException =>
        UDAggregate.createBroken(name, argTypes, returnType, initcond, reason)
    }
  }

  private def executeQuery(query: String, params: Object*) = {
    val statement = session.prepare(query)
    val boundStatement = statement.bind(params: _*)
    session.execute(boundStatement)
  }

  private def getFrozenTextMap(row: Row, column: String): util.Map[String, String] = {
    getFrozenMap(row, column, UTF8Type.instance, UTF8Type.instance)
  }

  private def getFrozenMap[K, V](row: Row, column: String, keyType: AbstractType[K], valueType: AbstractType[V]): util.Map[K, V] = {
    Option(row.getBytesUnsafe(column))
      .map { b =>
        MapType.getInstance(keyType, valueType, false).compose(b)
      }
      .orNull
  }

  def getFrozenSet[T](row: Row, column: String, `type`: AbstractType[T]): util.Set[T] = {
    val raw = row.getBytesUnsafe(column)
    if (raw == null) null
    else SetType.getInstance(`type`, false).compose(raw)
  }

  private def getFrozenList[T](row: Row, column: String, `type`: AbstractType[T]) = {
    Option(row.getBytesUnsafe(column))
      .map(ListType.getInstance(`type`, false).compose)
      .orNull
  }

  override def close(): Unit = {
    //session.close()
    //cluster.close()
  }
}
