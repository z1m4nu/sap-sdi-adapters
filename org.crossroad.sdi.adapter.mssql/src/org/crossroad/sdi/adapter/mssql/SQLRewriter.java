/**
 * 
 */
package org.crossroad.sdi.adapter.mssql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.impl.ColumnHelper;
import org.crossroad.sdi.adapter.impl.FUNCTIONS;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;
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
public class SQLRewriter {
	private Logger logger = LogManager.getLogger(SQLRewriter.class);
	private final int maxIdentifierLength;
	private final Map<String, String> schemaAliasReplacements = new HashMap<String, String>();
	private char aliasSeed = 'A';
	private ExpressionBase.Type queryType = ExpressionBase.Type.QUERY;
	private Map<String, String> aliasMap = new HashMap<String, String>();
	private boolean isDateTime = false;
	private boolean isSmallDateTime = false;
	private boolean isBinary = false;
	private boolean isVarBinary = false;
	private Set<String> datetimeCols = new HashSet<String>();
	private Set<String> smalldatetimeCols = new HashSet<String>();
	private Set<String> binaryCols = new HashSet<String>();
	private Set<String> varbinaryCols = new HashSet<String>();
	public SQLRewriter(int maxIdentifierLength) {
		this.maxIdentifierLength = maxIdentifierLength;
	}

	public void setColumnHelper(ColumnHelper helper) {
	}

	public SQLRewriter(int maxIdentifierLength, String schemaAlias, String schemaAliasReplacement) {
		this.maxIdentifierLength = maxIdentifierLength;
		if (schemaAlias != null) {
			this.schemaAliasReplacements.put(schemaAlias, schemaAliasReplacement);
		}
	}

	public ExpressionBase.Type getQueryType() {
		return this.queryType;
	}

	public void setQueryType(ExpressionBase.Type type) {
		this.queryType = type;
	}

	private void cleanUp() {
		if (this.datetimeCols != null) {
			this.datetimeCols.clear();
		}
		if (this.smalldatetimeCols != null) {
			this.smalldatetimeCols.clear();
		}
		if (this.binaryCols != null) {
			this.binaryCols.clear();
		}
		if (this.varbinaryCols != null) {
			this.varbinaryCols.clear();
		}
	}

	public String rewriteSQL(String sql, Map<String, Set<String>> specialTypes, ColumnHelper helper)
			throws AdapterException {

		logger.debug("Rewrite SQL [" + sql + "]");

		cleanUp();

		if (specialTypes != null) {
			if (specialTypes.containsKey("DATETIME")) {
				this.datetimeCols = specialTypes.get("DATETIME");
			}
			if (specialTypes.containsKey("SMALLDATETIME")) {
				this.smalldatetimeCols = specialTypes.get("SMALLDATETIME");
			}
			if (specialTypes.containsKey("BINARY")) {
				this.binaryCols = specialTypes.get("BINARY");
			}
			if (specialTypes.containsKey("VARBINARY")) {
				this.varbinaryCols = specialTypes.get("VARBINARY");
			}
		}
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		try {
			ExpressionBase query = ExpressionParserUtil.buildQuery(sql, messageList);
			if (query != null) {
				setQueryType(query.getType());
				String sqlRewrite = regenerateSQL(query);
				logger.debug("SQL Generated [" + sqlRewrite + "]");

				return sqlRewrite;
			}
			for (ExpressionParserMessage e : messageList) {
				this.logger.error(e.getText());
			}
			throw new AdapterException("Parse failed. See earlier logs");
		} catch (Exception e) {
			this.logger.error("SQL Rewrite failed.", e);
			throw new AdapterException(e, "Parser failed. See earlier logs");
		}
	}

	private String regenerateSQL(ExpressionBase query) throws AdapterException {
		if (query.getType() == ExpressionBase.Type.SELECT) {
			return regenerateSQL((Query) query);
		}
		if (query.getType() == ExpressionBase.Type.INSERT) {
			return regenerateInsertSQL((Query) query);
		}
		if (query.getType() == ExpressionBase.Type.DELETE) {
			return regenerateDeleteSQL((Query) query);
		}
		if (query.getType() == ExpressionBase.Type.UPDATE) {
			return regenerateUpdateSQL((Query) query);
		}
		StringBuffer str = new StringBuffer();
		Expression exp = (Expression) query;
		str.append(expressionBuilder((ExpressionBase) exp.getOperands().get(0)));
		str.append(" ");
		str.append(printSetOperation(query.getType()));
		str.append(" ");
		str.append(expressionBuilder((ExpressionBase) exp.getOperands().get(1)));
		return str.toString();
	}

	public String regenerateInsertSQL(Query query) throws AdapterException {
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

	private String regenerateDeleteSQL(Query query) throws AdapterException {
		StringBuffer sql = new StringBuffer();
		sql.append("DELETE FROM ");
		sql.append(expressionBuilder(query.getFromClause()));
		if (query.getWhereClause() != null) {
			sql.append(" WHERE ");
			sql.append(whereClauseBuilder(query.getWhereClause()));
		}
		return sql.toString();
	}

	private String regenerateUpdateSQL(Query query) throws AdapterException {
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

	private String regenerateSQL(Query query) throws AdapterException {
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

	private String clauseBuilder(String keyword, List<ExpressionBase> eprx) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(keyword);
		buffer.append(expressionBaseListBuilder(eprx));

		return buffer.toString();
	}

	private String fromClauseBuilder(ExpressionBase eprx) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		buffer.append(" FROM ");
		buffer.append(expressionBuilder(eprx));

		return buffer.toString();
	}

	private String groupByClauseBuilder(List<ExpressionBase> eprx) throws AdapterException {
		return clauseBuilder(" GROUP BY ", eprx).toString();
	}

	/**
	 * 
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	private String havingClauseBuilder(List<ExpressionBase> eprx) throws AdapterException {
		return clauseBuilder(" HAVING ", eprx).toString();
	}

	/**
	 * 
	 * @param order
	 * @return
	 * @throws Exception
	 */
	private String orderClauseBuilder(List<Order> order) throws AdapterException {
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

	/**
	 * Build the where clause
	 * 
	 * @param where
	 * @return
	 * @throws AdapterException
	 */
	private String whereClauseBuilder(List<ExpressionBase> where) throws AdapterException {
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

	private String setClauseBuilder(List<ExpressionBase> setclause) throws AdapterException {
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
		String colName = col.getColumnName().substring(1, col.getColumnName().length() - 1);
		checkSpecialType(colName);
		str.append(columnNameBuilder(col.getColumnName()));
		str.append(" = ");
		str.append(expressionBuilder(col.getColumnValueExp()));
		return str.toString();
	}

	protected String aliasRewriter(String alias) {
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

	protected String expressionBuilder(ExpressionBase val) throws AdapterException {
		StringBuffer str = new StringBuffer();
		logger.debug("Expression type [" + val.getType().name() + "]");
		switch (val.getType()) {
		case ALL:
			str.append(printExpressionList((Expression) val));
			break;
		case COLUMN:
			ColumnReference columnReference = (ColumnReference) val;
			if (columnReference.getTableName() != null) {
				str.append(columnNameBuilder(aliasRewriter(columnReference.getTableName())) + ".");
			}
			checkSpecialType(columnReference.getColumnName());
			str.append(columnNameBuilder(columnReference.getColumnName()));
			break;
		case AND:
			str.append(tableNameBuilder((TableReference) val));
			break;
		case TABLE:
			str.append(tableNameBuilder((TableReference) val));
			break;
		case SUBQUERY:
			str.append(" ( ");
			str.append(regenerateSQL((Query) val));
			str.append(" ) ");
			break;
		case TIMESTAMP_LITERAL:
		case DATE_LITERAL:
		case TIME_LITERAL:
			str.append(printDT((Expression) val));
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
			str.append(twoMembersBuilder((Expression) val));
			break;
		case IN:
		case NOT_IN:
			str.append(statementINBuilder((Expression) val));
			break;
		case CHARACTER_LITERAL:
		case INT_LITERAL:
		case FLOAT_LITERAL:
			str.append(((Expression) val).getValue());
			break;
		case OR:
			str.append(printOr((Expression) val));
			break;
		case FUNCTION:
			str.append(printFunction((Expression) val));
			break;
		case LEFT_OUTER_JOIN:
		case INNER_JOIN:
			str.append(statementJoinBuilder((Join) val));
			break;
		case LIKE:
		case NOT_LIKE:
			str.append(statementLIKEBuilder((Expression) val));
			break;
		case IS_NULL:
		case IS_NOT_NULL:
			str.append(statementNULLBuilder((Expression)val));
			break;
		case UNION_ALL:
		case UNION_DISTINCT:
		case INTERSECT:
		case EXCEPT:
			str.append(statementUNIONBuilder((Expression)val));
			break;
		case SELECT:
		case QUERY:
			str.append(statementSUBQUERYBuilder(((Expression)val)));
			break;
		case DISTINCT:
			statementDISTINCTBuilder((Expression)val);
			break;
		case BETWEEN:
		case NOT_BETWEEN:
			str.append(statementBETWEENBuilder((Expression)val));
			break;
		case CONCAT:
			str.append(statementCONCAT((Expression)val));
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
			str.append(aliasRewriter(val.getAlias()));
		}
		return str.toString();
	}

	private String statementBETWEENBuilder(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		if (Type.BETWEEN.equals(expr.getType()))
		{
			buffer.append(" BETWEEN ");
		} else 	if (Type.NOT_BETWEEN.equals(expr.getType()))
		{
			buffer.append(" NOT BETWEEN ");
			
		} else {
			throw new AdapterException("Expression type [" + expr.getType().name()
					+ "] not recognize as 'BETWEEN|NOT BETWEEN' statement.");			
		}

		boolean first = true;
		for(ExpressionBase base:expr.getOperands())
		{
			if( first)
			{
				first = false;
			} else {
				buffer.append(" AND ");
			}
			
			buffer.append(expressionBuilder(base));
		}

		return buffer.toString();
	}

	private String statementCONCAT(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		
		buffer.append("CONCAT (");
		boolean first = true;
		for (ExpressionBase base: expr.getOperands())
		{
			if (first)
			{
				first = false;
			} else {
				buffer.append(',');
			}
			buffer.append(expressionBuilder(base));
		}
		buffer.append(")");
		return buffer.toString();
	}
	
	private String statementDISTINCTBuilder(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append("DISTINCT ");
		for(ExpressionBase base:expr.getOperands())
		{
			buffer.append(expressionBuilder(base));
		}
		buffer.append(")");

		return buffer.toString();
	}

	private String statementSUBQUERYBuilder(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append("(");
		buffer.append(regenerateSQL(expr.getOperands().get(0)));
		buffer.append(")");

		return buffer.toString();
	}

	private String statementUNIONBuilder(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.UNION_ALL.equals(expr.getType())) {
			buffer.append(" UNION ALL ");
		} else if (Type.UNION_DISTINCT.equals(expr.getType())) {
			buffer.append(" UNION ");
		} else if (Type.INTERSECT.equals(expr.getType())){
			buffer.append(" INTERSECT ");
		} else if (Type.EXCEPT.equals(expr.getType())){
			buffer.append(" EXCEPT ");
		} else {
			throw new AdapterException("Expression type [" + expr.getType().name()
					+ "] not recognize as 'UNION ALL|UNION' statement.");
		}

		buffer.append(regenerateSQL(expr.getOperands().get(1)));
		
		return buffer.toString();
	}
	private String statementNULLBuilder(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		int size = expr.getOperands().size();

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.IN.equals(expr.getType())) {
			buffer.append(" IS NULL ");
		} else if (Type.NOT_IN.equals(expr.getType())) {
			buffer.append(" IS NOT NULL ");
		} else {
			throw new AdapterException("Expression type [" + expr.getType().name()
					+ "] not recognize as 'IS NULL| IS NOT NULL' statement.");
		}

		if (size > 1) {
			buffer.append(expressionBuilder(expr.getOperands().get(1)));
		}
		return buffer.toString();
	}

	/**
	 * 
	 * @param join
	 * @return
	 * @throws AdapterException
	 */
	private String statementJoinBuilder(Join join) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append("(");
		buffer.append(expressionBuilder(join.getLeftNode()));
		if (join.getType() == ExpressionBase.Type.INNER_JOIN) {
			buffer.append(" INNER JOIN ");
		} else if (Type.LEFT_OUTER_JOIN.equals(join.getType())) {
			buffer.append(" LEFT OUTER JOIN ");
		} else {
			throw new AdapterException("Expression type [" + join.getType().name()
					+ "] not recognize as 'INNER JOIN|LEFT OUTER JOIN' statement.");
		}
		buffer.append(expressionBuilder(join.getRightNode()));
		buffer.append(" ON (");
		buffer.append(expressionBuilder(join.getJoinCondition()));
		buffer.append("))");

		return buffer.toString();
	}

	private String statementLIKEBuilder(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.LIKE.equals(expr.getType())) {
			buffer.append(" LIKE ");
		} else if (Type.NOT_LIKE.equals(expr.getType())) {
			buffer.append(" NOT LIKE ");
		} else {
			throw new AdapterException(
					"Expression type [" + expr.getType().name() + "] not recognize as 'LIKE|NOT LIKE' statement.");
		}

		buffer.append(expressionBuilder(expr.getOperands().get(1)));

		return buffer.toString();
	}

	private String statementINBuilder(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.IN.equals(expr.getType())) {
			buffer.append(" IN ");
		} else if (Type.NOT_IN.equals(expr.getType())) {
			buffer.append(" NOT IN ");
		} else {
			throw new AdapterException(
					"Expression type [" + expr.getType().name() + "] not recognize as 'IN|NOT IN' statement.");
		}

		buffer.append("(");
		buffer.append(expressionBuilder(expr.getOperands().get(1)));
		buffer.append(")");

		return buffer.toString();
	}

	private String printExpressionList(Expression expr) throws AdapterException {
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

	private String expressionBaseListBuilder(List<ExpressionBase> proj) throws AdapterException {
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

	private String printDT(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();

		buffer.setLength(0);

		String _v = ((Expression) expr.getOperands().get(0)).getValue();

		switch (expr.getType()) {
		case TIMESTAMP_LITERAL:
			//buffer.append("{ts");
			buffer.append("convert(datetime,");
			buffer.append(MSSQLAdapterUtil.buidTS(MSSQLAdapterUtil.str2DT(_v)));
			buffer.append(")");
			//buffer.append("}");
			break;
		default:
			throw new AdapterException("Expression type [" + expr.getType().name() + "] is not supported.");
		}

		return buffer.toString();
	}

	private String printFxColumns(Expression expr) throws AdapterException {
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

	private String printOr(Expression expr) throws AdapterException {
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

	private String printFunction(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);
		FUNCTIONS fx = FUNCTIONS.valueOf(expr.getValue());
		try {
			switch (fx) {
			case TO_DOUBLE:
				buffer.append("CAST(");
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				buffer.append(" AS DOUBLE PRECISION");
				buffer.append(")");
				break;
			case TO_DECIMAL:
				List<ExpressionBase> params = expr.getOperands();
				if (params.size() < 3) {
					buffer.append("CAST(");
					buffer.append(expressionBuilder(expr.getOperands().get(0)));
					buffer.append(" AS DECIMAL");
					buffer.append(")");
				} else {
					buffer.append("CAST(");
					buffer.append(expressionBuilder(expr.getOperands().get(0)));
					buffer.append(" AS DECIMAL(");
					buffer.append(expressionBuilder(expr.getOperands().get(1)));
					buffer.append(",");
					buffer.append(expressionBuilder(expr.getOperands().get(2)));
					buffer.append(")");
					buffer.append(")");
				}
				break;
			case SUM:
			case COUNT:
			case MIN:
			case MAX:
				buffer.append(printFxColumns(expr));
				break;
			case TO_REAL:
			case TO_INT:
			case TO_INTEGER:
			case TO_SMALLINT:
			case TO_TINYINT:
			case TO_BIGINT:
			case TO_TIMESTAMP:
				buffer.append("CAST(");
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				buffer.append(" AS ");
				buffer.append(fx.suffix());
				buffer.append(")");
				break;
			case MOD:
				buffer.append("(").append(expressionBuilder(expr.getOperands().get(0))).append(" % ")
						.append(expressionBuilder(expr.getOperands().get(1))).append(")");
				break;
			case CEIL:
				buffer.append("CEILING(");
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				buffer.append(")");
				break;
			case LN:
				buffer.append("LOG(");
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				buffer.append(")");
				break;
			case LOG:
				buffer.append("LOG(");
				buffer.append(expressionBuilder(expr.getOperands().get(1)));
				buffer.append(", ");
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				buffer.append(")");
				break;
			case ATAN2:
			case STDDEV:
			case AVG:
				if (expr.getValue().equals("ATAN2")) {
					buffer.append("ATN2 (");
				} else if (expr.getValue().equals("STDDEV")) {
					buffer.append("STDEV (");
				} else if (expr.getValue().equals("AVG")) {
					buffer.append("AVG (");
				} else {
					buffer.append(expr.getValue() + "(");
				}
				boolean first = true;
				for (ExpressionBase param : expr.getOperands()) {
					if (first) {
						if (param.getType() == ExpressionBase.Type.DISTINCT) {
							buffer.append("DISTINCT ");
							continue;
						}
						first = false;
					} else {
						buffer.append(", ");
					}
					buffer.append(expressionBuilder(param));
				}
				if (expr.getValue().equals("AVG")) {
					buffer.append(" * 1.0 )");
				} else {
					buffer.append(")");
				}
				break;
			case TO_VARCHAR:
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				/*
				 * if (columnHelper != null) { buffer.append("CAST (");
				 * buffer.append(printExpression(expr.getOperands().get(0)));
				 * buffer.append(" AS VARCHAR)"); } else { buffer.append("-- " +
				 * expr.getValue()); }
				 */
				break;
			default:
				throw new AdapterException("Function [" + expr.getValue() + "] is not supported.");
			}
		} catch (Exception e) {
			throw new AdapterException(e, "Error while building function [" + expr.getValue() + "].");
		}

		return buffer.toString();
	}

	/**
	 * Build statement containing two expression separate by a sign or something
	 * else
	 * 
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	private String twoMembersBuilder(Expression expr) throws AdapterException {
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
	 * 
	 * @param tabRef
	 * @return
	 * @throws AdapterException
	 */
	private String tableNameBuilder(TableReference tabRef) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		String tabName = tabRef.getName();
		if (tabName.contains(".")) {
			buffer.append(MSSQLAdapterUtil.SQLTableBuilder(UniqueNameTools.build(tabName)));
			//buffer.append(UniqueNameTools.build(tabName).getUniqueName());
		} else if (tabRef.getDatabase() != null) {
			buffer.append("[");
			buffer.append(tabRef.getDatabase());
			buffer.append("].");
		}
		return buffer.toString();
	}
	/**
	 * 
	 * @param columnName
	 * @return
	 */
	private String columnNameBuilder(String columnName) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[");
		buffer.append(columnName.replaceAll("\"", ""));
		buffer.append("]");
		return buffer.toString();
	}

	private static String printSetOperation(ExpressionBase.Type type) throws AdapterException {
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

	private void checkSpecialType(String columnName) {
		String colName = columnName.replaceAll("\"", "");
		if ((this.datetimeCols != null) && (this.datetimeCols.contains(colName))) {
			this.isDateTime = true;
		}
		if ((this.smalldatetimeCols != null) && (this.smalldatetimeCols.contains(colName))) {
			this.isSmallDateTime = true;
		}
		if ((this.binaryCols != null) && (this.binaryCols.contains(colName))) {
			this.isBinary = true;
		}
		if ((this.varbinaryCols != null) && (this.varbinaryCols.contains(colName))) {
			this.isVarBinary = true;
		}
	}
}
