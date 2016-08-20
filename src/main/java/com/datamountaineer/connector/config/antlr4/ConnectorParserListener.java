// Generated from ConnectorParser.g4 by ANTLR 4.5.3
package com.datamountaineer.connector.config.antlr4;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ConnectorParser}.
 */
public interface ConnectorParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#stat}.
	 * @param ctx the parse tree
	 */
	void enterStat(ConnectorParser.StatContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#stat}.
	 * @param ctx the parse tree
	 */
	void exitStat(ConnectorParser.StatContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#into}.
	 * @param ctx the parse tree
	 */
	void enterInto(ConnectorParser.IntoContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#into}.
	 * @param ctx the parse tree
	 */
	void exitInto(ConnectorParser.IntoContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#pk}.
	 * @param ctx the parse tree
	 */
	void enterPk(ConnectorParser.PkContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#pk}.
	 * @param ctx the parse tree
	 */
	void exitPk(ConnectorParser.PkContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#insert_into}.
	 * @param ctx the parse tree
	 */
	void enterInsert_into(ConnectorParser.Insert_intoContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#insert_into}.
	 * @param ctx the parse tree
	 */
	void exitInsert_into(ConnectorParser.Insert_intoContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#upsert_into}.
	 * @param ctx the parse tree
	 */
	void enterUpsert_into(ConnectorParser.Upsert_intoContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#upsert_into}.
	 * @param ctx the parse tree
	 */
	void exitUpsert_into(ConnectorParser.Upsert_intoContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#upsert_pk_into}.
	 * @param ctx the parse tree
	 */
	void enterUpsert_pk_into(ConnectorParser.Upsert_pk_intoContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#upsert_pk_into}.
	 * @param ctx the parse tree
	 */
	void exitUpsert_pk_into(ConnectorParser.Upsert_pk_intoContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#sql_action}.
	 * @param ctx the parse tree
	 */
	void enterSql_action(ConnectorParser.Sql_actionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#sql_action}.
	 * @param ctx the parse tree
	 */
	void exitSql_action(ConnectorParser.Sql_actionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#schema_name}.
	 * @param ctx the parse tree
	 */
	void enterSchema_name(ConnectorParser.Schema_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#schema_name}.
	 * @param ctx the parse tree
	 */
	void exitSchema_name(ConnectorParser.Schema_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#insert_from_clause}.
	 * @param ctx the parse tree
	 */
	void enterInsert_from_clause(ConnectorParser.Insert_from_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#insert_from_clause}.
	 * @param ctx the parse tree
	 */
	void exitInsert_from_clause(ConnectorParser.Insert_from_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#select_clause}.
	 * @param ctx the parse tree
	 */
	void enterSelect_clause(ConnectorParser.Select_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#select_clause}.
	 * @param ctx the parse tree
	 */
	void exitSelect_clause(ConnectorParser.Select_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#select_clause_basic}.
	 * @param ctx the parse tree
	 */
	void enterSelect_clause_basic(ConnectorParser.Select_clause_basicContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#select_clause_basic}.
	 * @param ctx the parse tree
	 */
	void exitSelect_clause_basic(ConnectorParser.Select_clause_basicContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#topic_name}.
	 * @param ctx the parse tree
	 */
	void enterTopic_name(ConnectorParser.Topic_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#topic_name}.
	 * @param ctx the parse tree
	 */
	void exitTopic_name(ConnectorParser.Topic_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#table_name}.
	 * @param ctx the parse tree
	 */
	void enterTable_name(ConnectorParser.Table_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#table_name}.
	 * @param ctx the parse tree
	 */
	void exitTable_name(ConnectorParser.Table_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#column_name}.
	 * @param ctx the parse tree
	 */
	void enterColumn_name(ConnectorParser.Column_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#column_name}.
	 * @param ctx the parse tree
	 */
	void exitColumn_name(ConnectorParser.Column_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#column_name_alias}.
	 * @param ctx the parse tree
	 */
	void enterColumn_name_alias(ConnectorParser.Column_name_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#column_name_alias}.
	 * @param ctx the parse tree
	 */
	void exitColumn_name_alias(ConnectorParser.Column_name_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#column_list}.
	 * @param ctx the parse tree
	 */
	void enterColumn_list(ConnectorParser.Column_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#column_list}.
	 * @param ctx the parse tree
	 */
	void exitColumn_list(ConnectorParser.Column_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void enterFrom_clause(ConnectorParser.From_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void exitFrom_clause(ConnectorParser.From_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#ignored_name}.
	 * @param ctx the parse tree
	 */
	void enterIgnored_name(ConnectorParser.Ignored_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#ignored_name}.
	 * @param ctx the parse tree
	 */
	void exitIgnored_name(ConnectorParser.Ignored_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#ignore_clause}.
	 * @param ctx the parse tree
	 */
	void enterIgnore_clause(ConnectorParser.Ignore_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#ignore_clause}.
	 * @param ctx the parse tree
	 */
	void exitIgnore_clause(ConnectorParser.Ignore_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#pk_name}.
	 * @param ctx the parse tree
	 */
	void enterPk_name(ConnectorParser.Pk_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#pk_name}.
	 * @param ctx the parse tree
	 */
	void exitPk_name(ConnectorParser.Pk_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#primary_key_list}.
	 * @param ctx the parse tree
	 */
	void enterPrimary_key_list(ConnectorParser.Primary_key_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#primary_key_list}.
	 * @param ctx the parse tree
	 */
	void exitPrimary_key_list(ConnectorParser.Primary_key_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#autocreate}.
	 * @param ctx the parse tree
	 */
	void enterAutocreate(ConnectorParser.AutocreateContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#autocreate}.
	 * @param ctx the parse tree
	 */
	void exitAutocreate(ConnectorParser.AutocreateContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#autoevolve}.
	 * @param ctx the parse tree
	 */
	void enterAutoevolve(ConnectorParser.AutoevolveContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#autoevolve}.
	 * @param ctx the parse tree
	 */
	void exitAutoevolve(ConnectorParser.AutoevolveContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#batch_size}.
	 * @param ctx the parse tree
	 */
	void enterBatch_size(ConnectorParser.Batch_sizeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#batch_size}.
	 * @param ctx the parse tree
	 */
	void exitBatch_size(ConnectorParser.Batch_sizeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#batching}.
	 * @param ctx the parse tree
	 */
	void enterBatching(ConnectorParser.BatchingContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#batching}.
	 * @param ctx the parse tree
	 */
	void exitBatching(ConnectorParser.BatchingContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#capitalize}.
	 * @param ctx the parse tree
	 */
	void enterCapitalize(ConnectorParser.CapitalizeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#capitalize}.
	 * @param ctx the parse tree
	 */
	void exitCapitalize(ConnectorParser.CapitalizeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#partition_name}.
	 * @param ctx the parse tree
	 */
	void enterPartition_name(ConnectorParser.Partition_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#partition_name}.
	 * @param ctx the parse tree
	 */
	void exitPartition_name(ConnectorParser.Partition_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#partition_list}.
	 * @param ctx the parse tree
	 */
	void enterPartition_list(ConnectorParser.Partition_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#partition_list}.
	 * @param ctx the parse tree
	 */
	void exitPartition_list(ConnectorParser.Partition_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#partitionby}.
	 * @param ctx the parse tree
	 */
	void enterPartitionby(ConnectorParser.PartitionbyContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#partitionby}.
	 * @param ctx the parse tree
	 */
	void exitPartitionby(ConnectorParser.PartitionbyContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#distribute_name}.
	 * @param ctx the parse tree
	 */
	void enterDistribute_name(ConnectorParser.Distribute_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#distribute_name}.
	 * @param ctx the parse tree
	 */
	void exitDistribute_name(ConnectorParser.Distribute_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#distribute_list}.
	 * @param ctx the parse tree
	 */
	void enterDistribute_list(ConnectorParser.Distribute_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#distribute_list}.
	 * @param ctx the parse tree
	 */
	void exitDistribute_list(ConnectorParser.Distribute_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#distributeby}.
	 * @param ctx the parse tree
	 */
	void enterDistributeby(ConnectorParser.DistributebyContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#distributeby}.
	 * @param ctx the parse tree
	 */
	void exitDistributeby(ConnectorParser.DistributebyContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#timestamp_clause}.
	 * @param ctx the parse tree
	 */
	void enterTimestamp_clause(ConnectorParser.Timestamp_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#timestamp_clause}.
	 * @param ctx the parse tree
	 */
	void exitTimestamp_clause(ConnectorParser.Timestamp_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#timestamp_value}.
	 * @param ctx the parse tree
	 */
	void enterTimestamp_value(ConnectorParser.Timestamp_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#timestamp_value}.
	 * @param ctx the parse tree
	 */
	void exitTimestamp_value(ConnectorParser.Timestamp_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storedas_name}.
	 * @param ctx the parse tree
	 */
	void enterStoredas_name(ConnectorParser.Storedas_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storedas_name}.
	 * @param ctx the parse tree
	 */
	void exitStoredas_name(ConnectorParser.Storedas_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storedas_value}.
	 * @param ctx the parse tree
	 */
	void enterStoredas_value(ConnectorParser.Storedas_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storedas_value}.
	 * @param ctx the parse tree
	 */
	void exitStoredas_value(ConnectorParser.Storedas_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#buckets_number}.
	 * @param ctx the parse tree
	 */
	void enterBuckets_number(ConnectorParser.Buckets_numberContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#buckets_number}.
	 * @param ctx the parse tree
	 */
	void exitBuckets_number(ConnectorParser.Buckets_numberContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#clusterby_name}.
	 * @param ctx the parse tree
	 */
	void enterClusterby_name(ConnectorParser.Clusterby_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#clusterby_name}.
	 * @param ctx the parse tree
	 */
	void exitClusterby_name(ConnectorParser.Clusterby_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#clusterby_list}.
	 * @param ctx the parse tree
	 */
	void enterClusterby_list(ConnectorParser.Clusterby_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#clusterby_list}.
	 * @param ctx the parse tree
	 */
	void exitClusterby_list(ConnectorParser.Clusterby_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#clusterby}.
	 * @param ctx the parse tree
	 */
	void enterClusterby(ConnectorParser.ClusterbyContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#clusterby}.
	 * @param ctx the parse tree
	 */
	void exitClusterby(ConnectorParser.ClusterbyContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_consumer_group}.
	 * @param ctx the parse tree
	 */
	void enterWith_consumer_group(ConnectorParser.With_consumer_groupContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_consumer_group}.
	 * @param ctx the parse tree
	 */
	void exitWith_consumer_group(ConnectorParser.With_consumer_groupContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_consumer_group_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_consumer_group_value(ConnectorParser.With_consumer_group_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_consumer_group_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_consumer_group_value(ConnectorParser.With_consumer_group_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#offset_partition_inner}.
	 * @param ctx the parse tree
	 */
	void enterOffset_partition_inner(ConnectorParser.Offset_partition_innerContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#offset_partition_inner}.
	 * @param ctx the parse tree
	 */
	void exitOffset_partition_inner(ConnectorParser.Offset_partition_innerContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#offset_partition}.
	 * @param ctx the parse tree
	 */
	void enterOffset_partition(ConnectorParser.Offset_partitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#offset_partition}.
	 * @param ctx the parse tree
	 */
	void exitOffset_partition(ConnectorParser.Offset_partitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#partition_offset_list}.
	 * @param ctx the parse tree
	 */
	void enterPartition_offset_list(ConnectorParser.Partition_offset_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#partition_offset_list}.
	 * @param ctx the parse tree
	 */
	void exitPartition_offset_list(ConnectorParser.Partition_offset_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_offset_list}.
	 * @param ctx the parse tree
	 */
	void enterWith_offset_list(ConnectorParser.With_offset_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_offset_list}.
	 * @param ctx the parse tree
	 */
	void exitWith_offset_list(ConnectorParser.With_offset_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#sample_clause}.
	 * @param ctx the parse tree
	 */
	void enterSample_clause(ConnectorParser.Sample_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#sample_clause}.
	 * @param ctx the parse tree
	 */
	void exitSample_clause(ConnectorParser.Sample_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#sample_value}.
	 * @param ctx the parse tree
	 */
	void enterSample_value(ConnectorParser.Sample_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#sample_value}.
	 * @param ctx the parse tree
	 */
	void exitSample_value(ConnectorParser.Sample_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#sample_period}.
	 * @param ctx the parse tree
	 */
	void enterSample_period(ConnectorParser.Sample_periodContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#sample_period}.
	 * @param ctx the parse tree
	 */
	void exitSample_period(ConnectorParser.Sample_periodContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_format_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_format_clause(ConnectorParser.With_format_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_format_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_format_clause(ConnectorParser.With_format_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_format}.
	 * @param ctx the parse tree
	 */
	void enterWith_format(ConnectorParser.With_formatContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_format}.
	 * @param ctx the parse tree
	 */
	void exitWith_format(ConnectorParser.With_formatContext ctx);
}