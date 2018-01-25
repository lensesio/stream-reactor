// Generated from ConnectorParser.g4 by ANTLR 4.7
package com.datamountaineer.kcql.antlr4;
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
	 * Enter a parse tree produced by {@link ConnectorParser#write_mode}.
	 * @param ctx the parse tree
	 */
	void enterWrite_mode(ConnectorParser.Write_modeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#write_mode}.
	 * @param ctx the parse tree
	 */
	void exitWrite_mode(ConnectorParser.Write_modeContext ctx);
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
	 * Enter a parse tree produced by {@link ConnectorParser#column}.
	 * @param ctx the parse tree
	 */
	void enterColumn(ConnectorParser.ColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#column}.
	 * @param ctx the parse tree
	 */
	void exitColumn(ConnectorParser.ColumnContext ctx);
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
	 * Enter a parse tree produced by {@link ConnectorParser#with_ignore}.
	 * @param ctx the parse tree
	 */
	void enterWith_ignore(ConnectorParser.With_ignoreContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_ignore}.
	 * @param ctx the parse tree
	 */
	void exitWith_ignore(ConnectorParser.With_ignoreContext ctx);
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
	 * Enter a parse tree produced by {@link ConnectorParser#initialize}.
	 * @param ctx the parse tree
	 */
	void enterInitialize(ConnectorParser.InitializeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#initialize}.
	 * @param ctx the parse tree
	 */
	void exitInitialize(ConnectorParser.InitializeContext ctx);
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
	 * Enter a parse tree produced by {@link ConnectorParser#timestamp_unit_clause}.
	 * @param ctx the parse tree
	 */
	void enterTimestamp_unit_clause(ConnectorParser.Timestamp_unit_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#timestamp_unit_clause}.
	 * @param ctx the parse tree
	 */
	void exitTimestamp_unit_clause(ConnectorParser.Timestamp_unit_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#timestamp_unit_value}.
	 * @param ctx the parse tree
	 */
	void enterTimestamp_unit_value(ConnectorParser.Timestamp_unit_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#timestamp_unit_value}.
	 * @param ctx the parse tree
	 */
	void exitTimestamp_unit_value(ConnectorParser.Timestamp_unit_valueContext ctx);
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
	 * Enter a parse tree produced by {@link ConnectorParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void enterLimit_clause(ConnectorParser.Limit_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void exitLimit_clause(ConnectorParser.Limit_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#limit_value}.
	 * @param ctx the parse tree
	 */
	void enterLimit_value(ConnectorParser.Limit_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#limit_value}.
	 * @param ctx the parse tree
	 */
	void exitLimit_value(ConnectorParser.Limit_valueContext ctx);
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
	 * Enter a parse tree produced by {@link ConnectorParser#with_unwrap_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_unwrap_clause(ConnectorParser.With_unwrap_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_unwrap_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_unwrap_clause(ConnectorParser.With_unwrap_clauseContext ctx);
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
	 * Enter a parse tree produced by {@link ConnectorParser#with_structure}.
	 * @param ctx the parse tree
	 */
	void enterWith_structure(ConnectorParser.With_structureContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_structure}.
	 * @param ctx the parse tree
	 */
	void exitWith_structure(ConnectorParser.With_structureContext ctx);
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
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#project_to}.
	 * @param ctx the parse tree
	 */
	void enterProject_to(ConnectorParser.Project_toContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#project_to}.
	 * @param ctx the parse tree
	 */
	void exitProject_to(ConnectorParser.Project_toContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#version_number}.
	 * @param ctx the parse tree
	 */
	void enterVersion_number(ConnectorParser.Version_numberContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#version_number}.
	 * @param ctx the parse tree
	 */
	void exitVersion_number(ConnectorParser.Version_numberContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storeas_clause}.
	 * @param ctx the parse tree
	 */
	void enterStoreas_clause(ConnectorParser.Storeas_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storeas_clause}.
	 * @param ctx the parse tree
	 */
	void exitStoreas_clause(ConnectorParser.Storeas_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storeas_type}.
	 * @param ctx the parse tree
	 */
	void enterStoreas_type(ConnectorParser.Storeas_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storeas_type}.
	 * @param ctx the parse tree
	 */
	void exitStoreas_type(ConnectorParser.Storeas_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storeas_parameters}.
	 * @param ctx the parse tree
	 */
	void enterStoreas_parameters(ConnectorParser.Storeas_parametersContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storeas_parameters}.
	 * @param ctx the parse tree
	 */
	void exitStoreas_parameters(ConnectorParser.Storeas_parametersContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storeas_parameters_tuple}.
	 * @param ctx the parse tree
	 */
	void enterStoreas_parameters_tuple(ConnectorParser.Storeas_parameters_tupleContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storeas_parameters_tuple}.
	 * @param ctx the parse tree
	 */
	void exitStoreas_parameters_tuple(ConnectorParser.Storeas_parameters_tupleContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storeas_parameter}.
	 * @param ctx the parse tree
	 */
	void enterStoreas_parameter(ConnectorParser.Storeas_parameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storeas_parameter}.
	 * @param ctx the parse tree
	 */
	void exitStoreas_parameter(ConnectorParser.Storeas_parameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#storeas_value}.
	 * @param ctx the parse tree
	 */
	void enterStoreas_value(ConnectorParser.Storeas_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#storeas_value}.
	 * @param ctx the parse tree
	 */
	void exitStoreas_value(ConnectorParser.Storeas_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_tags}.
	 * @param ctx the parse tree
	 */
	void enterWith_tags(ConnectorParser.With_tagsContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_tags}.
	 * @param ctx the parse tree
	 */
	void exitWith_tags(ConnectorParser.With_tagsContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_key}.
	 * @param ctx the parse tree
	 */
	void enterWith_key(ConnectorParser.With_keyContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_key}.
	 * @param ctx the parse tree
	 */
	void exitWith_key(ConnectorParser.With_keyContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_key_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_key_value(ConnectorParser.With_key_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_key_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_key_value(ConnectorParser.With_key_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#key_delimiter}.
	 * @param ctx the parse tree
	 */
	void enterKey_delimiter(ConnectorParser.Key_delimiterContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#key_delimiter}.
	 * @param ctx the parse tree
	 */
	void exitKey_delimiter(ConnectorParser.Key_delimiterContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#key_delimiter_value}.
	 * @param ctx the parse tree
	 */
	void enterKey_delimiter_value(ConnectorParser.Key_delimiter_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#key_delimiter_value}.
	 * @param ctx the parse tree
	 */
	void exitKey_delimiter_value(ConnectorParser.Key_delimiter_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_inc_mode}.
	 * @param ctx the parse tree
	 */
	void enterWith_inc_mode(ConnectorParser.With_inc_modeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_inc_mode}.
	 * @param ctx the parse tree
	 */
	void exitWith_inc_mode(ConnectorParser.With_inc_modeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#inc_mode}.
	 * @param ctx the parse tree
	 */
	void enterInc_mode(ConnectorParser.Inc_modeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#inc_mode}.
	 * @param ctx the parse tree
	 */
	void exitInc_mode(ConnectorParser.Inc_modeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_type}.
	 * @param ctx the parse tree
	 */
	void enterWith_type(ConnectorParser.With_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_type}.
	 * @param ctx the parse tree
	 */
	void exitWith_type(ConnectorParser.With_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_type_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_type_value(ConnectorParser.With_type_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_type_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_type_value(ConnectorParser.With_type_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_doc_type}.
	 * @param ctx the parse tree
	 */
	void enterWith_doc_type(ConnectorParser.With_doc_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_doc_type}.
	 * @param ctx the parse tree
	 */
	void exitWith_doc_type(ConnectorParser.With_doc_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#doc_type}.
	 * @param ctx the parse tree
	 */
	void enterDoc_type(ConnectorParser.Doc_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#doc_type}.
	 * @param ctx the parse tree
	 */
	void exitDoc_type(ConnectorParser.Doc_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_index_suffix}.
	 * @param ctx the parse tree
	 */
	void enterWith_index_suffix(ConnectorParser.With_index_suffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_index_suffix}.
	 * @param ctx the parse tree
	 */
	void exitWith_index_suffix(ConnectorParser.With_index_suffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#index_suffix}.
	 * @param ctx the parse tree
	 */
	void enterIndex_suffix(ConnectorParser.Index_suffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#index_suffix}.
	 * @param ctx the parse tree
	 */
	void exitIndex_suffix(ConnectorParser.Index_suffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_converter}.
	 * @param ctx the parse tree
	 */
	void enterWith_converter(ConnectorParser.With_converterContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_converter}.
	 * @param ctx the parse tree
	 */
	void exitWith_converter(ConnectorParser.With_converterContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_converter_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_converter_value(ConnectorParser.With_converter_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_converter_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_converter_value(ConnectorParser.With_converter_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_target}.
	 * @param ctx the parse tree
	 */
	void enterWith_target(ConnectorParser.With_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_target}.
	 * @param ctx the parse tree
	 */
	void exitWith_target(ConnectorParser.With_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_target_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_target_value(ConnectorParser.With_target_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_target_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_target_value(ConnectorParser.With_target_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_jms_selector}.
	 * @param ctx the parse tree
	 */
	void enterWith_jms_selector(ConnectorParser.With_jms_selectorContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_jms_selector}.
	 * @param ctx the parse tree
	 */
	void exitWith_jms_selector(ConnectorParser.With_jms_selectorContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#jms_selector_value}.
	 * @param ctx the parse tree
	 */
	void enterJms_selector_value(ConnectorParser.Jms_selector_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#jms_selector_value}.
	 * @param ctx the parse tree
	 */
	void exitJms_selector_value(ConnectorParser.Jms_selector_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#tag_definition}.
	 * @param ctx the parse tree
	 */
	void enterTag_definition(ConnectorParser.Tag_definitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#tag_definition}.
	 * @param ctx the parse tree
	 */
	void exitTag_definition(ConnectorParser.Tag_definitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#tag_key}.
	 * @param ctx the parse tree
	 */
	void enterTag_key(ConnectorParser.Tag_keyContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#tag_key}.
	 * @param ctx the parse tree
	 */
	void exitTag_key(ConnectorParser.Tag_keyContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#tag_value}.
	 * @param ctx the parse tree
	 */
	void enterTag_value(ConnectorParser.Tag_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#tag_value}.
	 * @param ctx the parse tree
	 */
	void exitTag_value(ConnectorParser.Tag_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#ttl_clause}.
	 * @param ctx the parse tree
	 */
	void enterTtl_clause(ConnectorParser.Ttl_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#ttl_clause}.
	 * @param ctx the parse tree
	 */
	void exitTtl_clause(ConnectorParser.Ttl_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#ttl_type}.
	 * @param ctx the parse tree
	 */
	void enterTtl_type(ConnectorParser.Ttl_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#ttl_type}.
	 * @param ctx the parse tree
	 */
	void exitTtl_type(ConnectorParser.Ttl_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_pipeline_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_pipeline_clause(ConnectorParser.With_pipeline_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_pipeline_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_pipeline_clause(ConnectorParser.With_pipeline_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#pipeline_value}.
	 * @param ctx the parse tree
	 */
	void enterPipeline_value(ConnectorParser.Pipeline_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#pipeline_value}.
	 * @param ctx the parse tree
	 */
	void exitPipeline_value(ConnectorParser.Pipeline_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_compression_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_compression_clause(ConnectorParser.With_compression_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_compression_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_compression_clause(ConnectorParser.With_compression_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_compression_type}.
	 * @param ctx the parse tree
	 */
	void enterWith_compression_type(ConnectorParser.With_compression_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_compression_type}.
	 * @param ctx the parse tree
	 */
	void exitWith_compression_type(ConnectorParser.With_compression_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_delay_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_delay_clause(ConnectorParser.With_delay_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_delay_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_delay_clause(ConnectorParser.With_delay_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_delay_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_delay_value(ConnectorParser.With_delay_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_delay_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_delay_value(ConnectorParser.With_delay_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_partitioner_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_partitioner_clause(ConnectorParser.With_partitioner_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_partitioner_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_partitioner_clause(ConnectorParser.With_partitioner_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_partitioner_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_partitioner_value(ConnectorParser.With_partitioner_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_partitioner_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_partitioner_value(ConnectorParser.With_partitioner_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_subscription_clause}.
	 * @param ctx the parse tree
	 */
	void enterWith_subscription_clause(ConnectorParser.With_subscription_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_subscription_clause}.
	 * @param ctx the parse tree
	 */
	void exitWith_subscription_clause(ConnectorParser.With_subscription_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link ConnectorParser#with_subscription_value}.
	 * @param ctx the parse tree
	 */
	void enterWith_subscription_value(ConnectorParser.With_subscription_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ConnectorParser#with_subscription_value}.
	 * @param ctx the parse tree
	 */
	void exitWith_subscription_value(ConnectorParser.With_subscription_valueContext ctx);
}