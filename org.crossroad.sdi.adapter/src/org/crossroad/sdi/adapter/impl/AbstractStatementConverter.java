/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Join;
import com.sap.hana.dp.adapter.sdk.parser.Order;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;

/**
 * @author e.soden
 *
 */
public abstract class AbstractStatementConverter {
	protected Logger logger = LogManager.getLogger(this.getClass());
	private final int maxIdentifierLength;
	private ExpressionBase.Type queryType = ExpressionBase.Type.QUERY;
	private ColumnHelper columnHelper = null;
	private Map<String, String> aliasMap = new HashMap<String, String>();
	private char aliasSeed = 'A';

	/**
	 * 
	 */
	public AbstractStatementConverter(int maxIdentifierLength) {
		this.maxIdentifierLength = maxIdentifierLength;
	}

	public String convertSQL(String sql, ColumnHelper helper) throws AdapterException {

		String statement = null;
		logger.debug("Rewrite SQL [" + sql + "]");

		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		try {
			ExpressionBase query = ExpressionParserUtil.buildQuery(sql, messageList);
			if (query != null) {
				statement = convertSQL((Query) query, helper);
			}
			if (!messageList.isEmpty()) {
				for (ExpressionParserMessage e : messageList) {
					this.logger.error(e.getText());
				}
				throw new AdapterException("Parse failed. See earlier logs");
			}
		} catch (Exception e) {
			this.logger.error("SQL Rewrite failed.", e);
			throw new AdapterException(e, "Parser failed. See earlier logs");
		}

		logger.debug("SQL Generated [" + statement + "]");

		return statement;
	}

	/**
	 * 
	 * @param query
	 * @param helper
	 * @return
	 */
	public String convertSQL(Query query, ColumnHelper helper) throws AdapterException {
		String statement = null;

		this.columnHelper = helper;

		statement = parseQuery(query);

		return statement;
	}

	/**
	 * 
	 * @param type
	 * @return
	 * @throws AdapterException
	 */
	protected String setStatement(ExpressionBase.Type type) throws AdapterException {
		String str = new String();
		switch (type) {
		case OR:
			str = "UNION ALL";
			break;
		case LESS_THAN:
			str = "UNION DISTINCT";
			break;
		case AND:
			str = "INTERSECT";
			break;
		case UNION_ALL:
			str = "EXCEPT";
			break;
		default:
			throw new AdapterException("Operation [" + type.name() + "] is not supported.");
		}
		return str;
	}

	/**
	 * Parse {@link Query} and return converted SQL
	 * 
	 * @param query
	 * @return
	 * @throws AdapterException
	 */
	protected String parseQuery(ExpressionBase query) throws AdapterException {
		String statement = null;

		switch (query.getType()) {
		case SELECT:
			statement = selectStatement((Query) query);
			break;
		case UPDATE:
			statement = updateStatement((Query) query);
			break;
		case INSERT:
			statement = insertStatement((Query) query);
			break;
		case DELETE:
			statement = deleteStatement((Query) query);
			break;
		default:
			StringBuffer str = new StringBuffer();
			Expression exp = (Expression) query;
			str.append(expressionBuilder((ExpressionBase) exp.getOperands().get(0)));
			str.append(" ");
			str.append(setStatement(query.getType()));
			str.append(" ");
			str.append(expressionBuilder((ExpressionBase) exp.getOperands().get(1)));

			statement = str.toString();
			break;
		}

		return statement;
	}





	
	/**
	 * Main method to generate insert statement
	 * 
	 * @param query
	 * @return
	 */
	protected String insertStatement(Query query) throws AdapterException {
		StringBuffer sql = new StringBuffer();

		sql.append("INSERT INTO ");
		sql.append(expressionBuilder(query.getFromClause()));
		if (query.getProjections() != null) {
			sql.append(" (");
			sql.append(expressionBaseListBuilder(query.getProjections()));
			sql.append(")");
		}
		if (query.getValueClause() != null) {
			sql.append(" VALUES ");
			sql.append("(");
			sql.append(expressionBaseListBuilder(query.getValueClause()));
			sql.append(")");
		}
		if (query.getSubquery() != null) {
			sql.append(expressionBuilder(query.getSubquery()));
		}
		return sql.toString();
	}

	/**
	 * Main method to generate update statement
	 * 
	 * @param query
	 * @return
	 */
	protected String updateStatement(Query query) throws AdapterException {
		StringBuffer sql = new StringBuffer();
		sql.append("UPDATE ");
		sql.append(expressionBuilder(query.getFromClause()));
		sql.append(" SET ");
		sql.append(setClauseBuilder(query.getProjections()));
		if (query.getWhereClause() != null) {
			sql.append(" WHERE ");
			sql.append(whereClauseBuilder(query.getWhereClause()));
		}
		return sql.toString();
	}

	/**
	 * Main method to generate delete statement
	 * 
	 * @param query
	 * @return
	 */
	protected String deleteStatement(Query query) throws AdapterException {
		StringBuffer sql = new StringBuffer();
		sql.append("DELETE FROM ");
		sql.append(expressionBuilder(query.getFromClause()));
		if (query.getWhereClause() != null) {
			sql.append(" WHERE ");
			sql.append(whereClauseBuilder(query.getWhereClause()));
		}
		return sql.toString();
	}

	/**
	 * Main method to generate select statement
	 * 
	 * @param query
	 * @return
	 */
	protected String selectStatement(Query query) throws AdapterException {
		StringBuffer sql = new StringBuffer();

		sql.append("SELECT ");
		if (query.getLimit() != null) {
			sql.append("TOP ");
			sql.append(query.getLimit());
			sql.append(" ");
		}
		if (query.getDistinct()) {
			sql.append("DISTINCT ");
		}
		sql.append(expressionBaseListBuilder(query.getProjections()));

		sql.append(fromClauseBuilder(query.getFromClause()));

		if (query.getWhereClause() != null) {
			sql.append(whereClauseBuilder(query.getWhereClause()));
		}
		if (query.getGroupBy() != null) {
			sql.append(groupByClauseBuilder(query.getGroupBy()));
		}
		if (query.getHavingClause() != null) {
			sql.append(havingClauseBuilder(query.getHavingClause()));
		}
		if (query.getOrderBy() != null) {
			sql.append(orderClauseBuilder(query.getOrderBy()));
		}
		return sql.toString();
	}

	/**
	 * Convert to SQL function
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String functionStatement(Expression expr) throws AdapterException;

	/**
	 * Generate the column name
	 * 
	 * @param columnName
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String getColumnString(String columnName) throws AdapterException;

	/**
	 * Generate table name according to the {@link TableReference}
	 * 
	 * @param tableRef
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String getTableName(TableReference tableRef) throws AdapterException;

	/**
	 * Convert the alias to your needs
	 * 
	 * @param alias
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String aliasMagnifier(String alias) throws AdapterException;

	/**
	 * Create the concat statement
	 * 
	 * @param concatExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String concatStatement(Expression concatExpr) throws AdapterException;

	/**
	 * Generate the between expression
	 * 
	 * @param betweenExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String betweenStatement(Expression betweenExpr) throws AdapterException;

	/**
	 * Generate the join expression
	 * 
	 * @param joinExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String joinStatement(Join joinExpr) throws AdapterException;

	/**
	 * Generate the like expression
	 * 
	 * @param likeExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String likeStatement(Expression likeExpr) throws AdapterException;

	/**
	 * Generate the null expression
	 * 
	 * @param nullExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String nullStatement(Expression nullExpr) throws AdapterException;

	/**
	 * Generate the UNION expression
	 * 
	 * @param unionExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String unionStatement(Expression unionExpr) throws AdapterException;

	/**
	 * Generate the a subquery expression
	 * 
	 * @param unionExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String subqueryStatement(Expression subqueryExpr) throws AdapterException;

	/**
	 * Generate the a distinct expression
	 * 
	 * @param unionExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String distinctStatement(Expression distinctExpr) throws AdapterException;

	/**
	 * Generate the a IN expression
	 * 
	 * @param unionExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String inStatement(Expression inExpr) throws AdapterException;

	/**
	 * Generate the a time expression
	 * 
	 * @param unionExpr
	 * @return
	 * @throws AdapterException
	 */
	protected abstract String timeStatement(Expression timeExpr) throws AdapterException;

	/**
	 * Parse an expression list
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	private String parseExpressionList(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		boolean first = true;

		if (expr.getOperands() == null || expr.getOperands().isEmpty()) {
			buffer.append(expr.getValue());
		} else {
			for (ExpressionBase b : expr.getOperands()) {
				if (first) {
					first = false;
				} else {
					buffer.append(",");
				}

				buffer.append(expressionBuilder(b));
			}
		}
		return buffer.toString();
	}

	/**
	 * 
	 * @param alias
	 * @return
	 * @throws AdapterException
	 */
	protected String aliasRewriter(String alias) throws AdapterException {
		return aliasMagnifier(aliasInternalRewriter(alias));
	}

	/**
	 * 
	 * @param alias
	 * @return
	 */
	private String aliasInternalRewriter(String alias) {
		if (alias.length() <= this.maxIdentifierLength) {
			return alias;
		}
		String newAlias = (String) this.aliasMap.get(alias);
		if (newAlias == null) {
			newAlias = String.valueOf(this.aliasSeed++);
			this.aliasMap.put(alias, newAlias);
		}
		return newAlias;
	}

	/**
	 * Generate two member values
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String generateTwoMemberValues(Expression expr) throws AdapterException {
		StringBuffer str = new StringBuffer();
		try {
			str.append(expressionBuilder((ExpressionBase) expr.getOperands().get(0)));
			str.append(" " + expr.getValue() + " ");
			str.append(expressionBuilder((ExpressionBase) expr.getOperands().get(1)));
		} catch (Exception e) {
			throw new AdapterException(e);
		}
		return str.toString();
	}

	/**
	 * Generate the OR condition
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	private String orCondition(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		boolean first = true;

		try {
			for (ExpressionBase b : expr.getOperands()) {
				if (first) {
					first = false;
				} else {
					buffer.append(" OR ");
				}

				buffer.append(expressionBuilder(b));

			}
		} catch (Exception e) {
			throw new AdapterException(e);
		}

		return buffer.toString();
	}

	/**
	 * Build the where clause
	 * 
	 * @param where
	 * @return
	 * @throws AdapterException
	 */
	protected String whereClauseBuilder(List<ExpressionBase> where) throws AdapterException {
		boolean first = true;
		StringBuffer str = new StringBuffer(" WHERE ");

		for (ExpressionBase exp : where) {
			if (!first) {
				str.append(" AND ");
			}
			str.append("(").append(expressionBuilder(exp)).append(")");
			if (first) {
				first = false;
			}
		}
		return str.toString();
	}

	/**
	 * 
	 * @param setclause
	 * @return
	 * @throws AdapterException
	 */
	protected String setClauseBuilder(List<ExpressionBase> setclause) throws AdapterException {
		boolean first = true;

		StringBuffer str = new StringBuffer();
		for (ExpressionBase exp : setclause) {
			if (!first) {
				str.append(", ");
			}
			str.append(assignClauseBuilder(exp));
			if (first) {
				first = false;
			}
		}
		return str.toString();
	}

	private String assignClauseBuilder(ExpressionBase exp) throws AdapterException {
		StringBuffer str = new StringBuffer();
		ColumnReference col = (ColumnReference) exp;

		str.append(getColumnString(col.getColumnName()));
		str.append(" = ");
		str.append(expressionBuilder(col.getColumnValueExp()));
		return str.toString();
	}

	/**
	 * 
	 * @param proj
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionBaseListBuilder(List<ExpressionBase> proj) throws AdapterException {
		boolean first = true;
		StringBuffer str = new StringBuffer();
		for (ExpressionBase exp : proj) {
			if (first) {
				first = false;
			} else {
				str.append(", ");
			}
			str.append(expressionBuilder(exp));
		}
		return str.toString();
	}

	/**
	 * Core method for expression determination
	 * 
	 * @param val
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionBuilder(ExpressionBase val) throws AdapterException {
		StringBuffer str = new StringBuffer();
		logger.debug("Expression type [" + val.getType().name() + "]");
		switch (val.getType()) {
		case ALL:
			str.append(parseExpressionList((Expression) val));
			break;
		case COLUMN:
			ColumnReference columnReference = (ColumnReference) val;
			if (columnReference.getTableName() != null) {
				str.append(getColumnString(aliasRewriter(columnReference.getTableName())) + ".");
			}

			str.append(getColumnString(columnReference.getColumnName()));
			break;
		case AND:
		case TABLE:
			str.append(getTableName((TableReference) val));
			break;
		case SUBQUERY:
			str.append(" ( ");
			str.append(parseQuery(((Query) val)));
			str.append(" ) ");
			break;
		case TIMESTAMP_LITERAL:
		case DATE_LITERAL:
		case TIME_LITERAL:
			str.append(timeStatement((Expression) val));
			break;
		case GREATER_THAN:
		case EQUAL:
		case DIVIDE:
		case GREATER_THAN_EQ:
		case SUBTRACT:
		case MULTIPLY:
		case NOT_EQUAL:
		case LESS_THAN:
		case ADD:
		case LESS_THAN_EQ:
			str.append(generateTwoMemberValues((Expression) val));
			break;
		case IN:
		case NOT_IN:
			str.append(inStatement((Expression) val));
			break;
		case CHARACTER_LITERAL:
		case INT_LITERAL:
		case FLOAT_LITERAL:
			str.append(((Expression) val).getValue());
			break;
		case OR:
			str.append(orCondition((Expression) val));
			break;
		case FUNCTION:
			str.append(functionStatement((Expression) val));
			break;
		case LEFT_OUTER_JOIN:
		case INNER_JOIN:
			str.append(joinStatement((Join) val));
			break;
		case LIKE:
		case NOT_LIKE:
			str.append(likeStatement((Expression) val));
			break;
		case IS_NULL:
		case IS_NOT_NULL:
			str.append(nullStatement((Expression) val));
			break;
		case UNION_ALL:
		case UNION_DISTINCT:
		case INTERSECT:
		case EXCEPT:
			str.append(unionStatement((Expression) val));
			break;
		case SELECT:
		case QUERY:
			str.append(subqueryStatement(((Expression) val)));
			break;
		case DISTINCT:
			distinctStatement((Expression) val);
			break;
		case BETWEEN:
		case NOT_BETWEEN:
			str.append(betweenStatement((Expression) val));
			break;
		case CONCAT:
			str.append(concatStatement((Expression) val));
			break;
		case NULL:
		case DELETE:
		case ORDER_BY:
		case VARIABLE:
		case ASSIGN:
		case CASE:
		case PARAMETER:
		case UNKNOWN:
		case UPDATE:
		case CASE_CLAUSE:
		case CASE_CLAUSES:
		case CASE_ELSE:
		//case ROW_NUMBER:
		case UNARY_POSITIVE:
		case INSERT:
		default:
			throw new AdapterException("Unknown value [" + ((Expression) val).getValue() + "]");
		}
		if (val.getAlias() != null) {
			str.append(" ");
			str.append(aliasRewriter(val.getAlias().replace("\"", "")));
		}
		return str.toString();
	}

	protected String getFxColumns(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		boolean first = true;

		try {
			FUNCTIONS fx = FUNCTIONS.valueOf(expr.getValue());

			buffer.append(fx.name());
			buffer.append("(");

			for (ExpressionBase b : expr.getOperands()) {
				if (first) {
					first = false;
				} else {
					buffer.append(", ");
				}

				buffer.append(expressionBuilder(b));

			}
			buffer.append(")");
		} catch (Exception e) {
			throw new AdapterException(e);
		}

		return buffer.toString();
	}

	/**
	 * 
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	protected String fromClauseBuilder(ExpressionBase eprx) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		buffer.append(" FROM ");
		buffer.append(expressionBuilder(eprx));

		return buffer.toString();
	}

	/**
	 * 
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	protected String groupByClauseBuilder(List<ExpressionBase> eprx) throws AdapterException {
		return clauseBuilder(" GROUP BY ", eprx).toString();
	}

	/**
	 * 
	 * @param keyword
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	protected String clauseBuilder(String keyword, List<ExpressionBase> eprx) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(keyword);
		buffer.append(expressionBaseListBuilder(eprx));

		return buffer.toString();
	}

	/**
	 * 
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	protected String havingClauseBuilder(List<ExpressionBase> eprx) throws AdapterException {
		return clauseBuilder(" HAVING ", eprx).toString();
	}

	/**
	 * 
	 * @param order
	 * @return
	 * @throws Exception
	 */
	protected String orderClauseBuilder(List<Order> order) throws AdapterException {
		boolean first = true;
		StringBuffer str = new StringBuffer();
		str.append(" ORDER BY ");

		for (Order o : order) {
			if (first) {
				first = false;
			} else {
				str.append(", ");
			}
			str.append(expressionBuilder(o.getExpression()));
			if (o.getOrderType() == Order.Type.ASC) {
				str.append(" ASC");
			} else {
				str.append(" DESC");
			}
		}
		return str.toString();
	}
}
