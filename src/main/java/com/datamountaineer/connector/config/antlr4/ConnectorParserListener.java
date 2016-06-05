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
}