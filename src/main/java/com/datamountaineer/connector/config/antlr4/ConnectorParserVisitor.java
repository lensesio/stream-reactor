// Generated from ConnectorParser.g4 by ANTLR 4.5.3
package com.datamountaineer.connector.config.antlr4;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ConnectorParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ConnectorParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#stat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStat(ConnectorParser.StatContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#into}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInto(ConnectorParser.IntoContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#pk}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPk(ConnectorParser.PkContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#insert_into}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsert_into(ConnectorParser.Insert_intoContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#upsert_into}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpsert_into(ConnectorParser.Upsert_intoContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#upsert_pk_into}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpsert_pk_into(ConnectorParser.Upsert_pk_intoContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#sql_action}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql_action(ConnectorParser.Sql_actionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#schema_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSchema_name(ConnectorParser.Schema_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#select_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_clause(ConnectorParser.Select_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#topic_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTopic_name(ConnectorParser.Topic_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#table_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_name(ConnectorParser.Table_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#column_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_name(ConnectorParser.Column_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#column_name_alias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_name_alias(ConnectorParser.Column_name_aliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#column_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_list(ConnectorParser.Column_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#from_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFrom_clause(ConnectorParser.From_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#ignored_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIgnored_name(ConnectorParser.Ignored_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#ignore_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIgnore_clause(ConnectorParser.Ignore_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#pk_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPk_name(ConnectorParser.Pk_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#primary_key_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimary_key_list(ConnectorParser.Primary_key_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#autocreate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAutocreate(ConnectorParser.AutocreateContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#autoevolve}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAutoevolve(ConnectorParser.AutoevolveContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#batch_size}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBatch_size(ConnectorParser.Batch_sizeContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#batching}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBatching(ConnectorParser.BatchingContext ctx);
	/**
	 * Visit a parse tree produced by {@link ConnectorParser#capitalize}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCapitalize(ConnectorParser.CapitalizeContext ctx);
}