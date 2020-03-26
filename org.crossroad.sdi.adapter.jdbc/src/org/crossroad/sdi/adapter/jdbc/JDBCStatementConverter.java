/**
 * 
 */
package org.crossroad.sdi.adapter.jdbc;

import java.util.List;

import org.crossroad.sdi.adapter.impl.AbstractStatementConverter;
import org.crossroad.sdi.adapter.impl.FUNCTIONS;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.Join;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;

/**
 * @author e.soden
 *
 */
public class JDBCStatementConverter extends AbstractStatementConverter {

	/**
	 * @param maxIdentifierLength
	 */
	public JDBCStatementConverter(int maxIdentifierLength) {
		super(maxIdentifierLength);

	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.AbstractStatementConverter#
	 * functionStatement(com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String functionStatement(Expression expr) throws AdapterException {
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
				buffer.append(getFxColumns(expr));
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#getColumnString
	 * (java.lang.String)
	 */
	@Override
	protected String getColumnString(String columnName) throws AdapterException {
		return columnName.replaceAll("\"", "");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#getTableName(
	 * com.sap.hana.dp.adapter.sdk.parser.TableReference)
	 */
	@Override
	protected String getTableName(TableReference tableRef) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		String tabName = tableRef.getName().replaceAll("\"","");
		
		logger.info("Database [" + tableRef.getDatabase() + "] Name ["+tableRef.getName()+"] Owner ["+tableRef.getOwner()+"] Alias ["+tableRef.getAlias()+"]");
		
		
		if (tabName.contains(".<none>.")) {
			buffer.append(tabName.replace(".<none>.", "."));
		} else {
			buffer.append(tabName);
		}
		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#aliasMagnifier(
	 * java.lang.String)
	 */
	@Override
	protected String aliasMagnifier(String alias) throws AdapterException {
		return alias.replaceAll("\"", "");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#concatStatement
	 * (com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String concatStatement(Expression concatExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append("CONCAT (");
		boolean first = true;
		for (ExpressionBase base : concatExpr.getOperands()) {
			if (first) {
				first = false;
			} else {
				buffer.append(',');
			}
			buffer.append(expressionBuilder(base));
		}
		buffer.append(")");
		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.AbstractStatementConverter#
	 * betweenStatement(com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String betweenStatement(Expression betweenExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		if (Type.BETWEEN.equals(betweenExpr.getType())) {
			buffer.append(" BETWEEN ");
		} else if (Type.NOT_BETWEEN.equals(betweenExpr.getType())) {
			buffer.append(" NOT BETWEEN ");

		} else {
			throw new AdapterException("Expression type [" + betweenExpr.getType().name()
					+ "] not recognize as 'BETWEEN|NOT BETWEEN' statement.");
		}

		boolean first = true;
		for (ExpressionBase base : betweenExpr.getOperands()) {
			if (first) {
				first = false;
			} else {
				buffer.append(" AND ");
			}

			buffer.append(expressionBuilder(base));
		}

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#joinStatement(
	 * com.sap.hana.dp.adapter.sdk.parser.Join)
	 */
	@Override
	protected String joinStatement(Join joinExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append("(");
		buffer.append(expressionBuilder(joinExpr.getLeftNode()));
		if (joinExpr.getType() == ExpressionBase.Type.INNER_JOIN) {
			buffer.append(" INNER JOIN ");
		} else if (Type.LEFT_OUTER_JOIN.equals(joinExpr.getType())) {
			buffer.append(" LEFT OUTER JOIN ");
		} else {
			throw new AdapterException("Expression type [" + joinExpr.getType().name()
					+ "] not recognize as 'INNER JOIN|LEFT OUTER JOIN' statement.");
		}
		buffer.append(expressionBuilder(joinExpr.getRightNode()));
		buffer.append(" ON (");
		buffer.append(expressionBuilder(joinExpr.getJoinCondition()));
		buffer.append("))");

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#likeStatement(
	 * com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String likeStatement(Expression likeExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(expressionBuilder(likeExpr.getOperands().get(0)));
		if (Type.LIKE.equals(likeExpr.getType())) {
			buffer.append(" LIKE ");
		} else if (Type.NOT_LIKE.equals(likeExpr.getType())) {
			buffer.append(" NOT LIKE ");
		} else {
			throw new AdapterException(
					"Expression type [" + likeExpr.getType().name() + "] not recognize as 'LIKE|NOT LIKE' statement.");
		}

		buffer.append(expressionBuilder(likeExpr.getOperands().get(1)));

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#nullStatement(
	 * com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String nullStatement(Expression nullExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		int size = nullExpr.getOperands().size();

		buffer.append(expressionBuilder(nullExpr.getOperands().get(0)));
		if (Type.IN.equals(nullExpr.getType())) {
			buffer.append(" IS NULL ");
		} else if (Type.NOT_IN.equals(nullExpr.getType())) {
			buffer.append(" IS NOT NULL ");
		} else {
			throw new AdapterException("Expression type [" + nullExpr.getType().name()
					+ "] not recognize as 'IS NULL| IS NOT NULL' statement.");
		}

		if (size > 1) {
			buffer.append(expressionBuilder(nullExpr.getOperands().get(1)));
		}
		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#unionStatement(
	 * com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String unionStatement(Expression unionExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(expressionBuilder(unionExpr.getOperands().get(0)));
		if (Type.UNION_ALL.equals(unionExpr.getType())) {
			buffer.append(" UNION ALL ");
		} else if (Type.UNION_DISTINCT.equals(unionExpr.getType())) {
			buffer.append(" UNION ");
		} else if (Type.INTERSECT.equals(unionExpr.getType())) {
			buffer.append(" INTERSECT ");
		} else if (Type.EXCEPT.equals(unionExpr.getType())) {
			buffer.append(" EXCEPT ");
		} else {
			throw new AdapterException("Expression type [" + unionExpr.getType().name()
					+ "] not recognize as 'UNION ALL|UNION' statement.");
		}

		buffer.append(parseQuery(unionExpr.getOperands().get(1)));

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.AbstractStatementConverter#
	 * subqueryStatement(com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String subqueryStatement(Expression subqueryExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append("(");
		buffer.append(parseQuery(subqueryExpr.getOperands().get(0)));
		buffer.append(")");

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.AbstractStatementConverter#
	 * distinctStatement(com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String distinctStatement(Expression distinctExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append("DISTINCT ");
		for (ExpressionBase base : distinctExpr.getOperands()) {
			buffer.append(expressionBuilder(base));
		}
		buffer.append(")");

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#inStatement(com
	 * .sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String inStatement(Expression inExpr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		buffer.append(expressionBuilder(inExpr.getOperands().get(0)));
		if (Type.IN.equals(inExpr.getType())) {
			buffer.append(" IN ");
		} else if (Type.NOT_IN.equals(inExpr.getType())) {
			buffer.append(" NOT IN ");
		} else {
			throw new AdapterException(
					"Expression type [" + inExpr.getType().name() + "] not recognize as 'IN|NOT IN' statement.");
		}

		buffer.append("(");
		buffer.append(expressionBuilder(inExpr.getOperands().get(1)));
		buffer.append(")");

		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractStatementConverter#timeStatement(
	 * com.sap.hana.dp.adapter.sdk.parser.Expression)
	 */
	@Override
	protected String timeStatement(Expression timeExpr) throws AdapterException {
		return ((Expression) timeExpr.getOperands().get(0)).getValue();
	}

}
