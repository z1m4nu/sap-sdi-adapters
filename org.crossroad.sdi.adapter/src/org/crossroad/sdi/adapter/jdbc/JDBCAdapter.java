package org.crossroad.sdi.adapter.jdbc;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Properties;

import org.crossroad.sdi.adapter.ClassUtil;
import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.AdapterConstants;
import org.crossroad.sdi.adapter.impl.SQLRewriter;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;

/**
 * This is a sample adapter that connects to a database with jdbc driver. You
 * can modify driver and url to connect to any database system. currently it is
 * pointing to SQL server
 */
public class JDBCAdapter extends AbstractJDBCAdapter {
	private JDBCStatementConverter rewriter = new JDBCStatementConverter(128);

	public Class getLoggerName() {
		return JDBCAdapter.class;
	}

	@Override
	protected void populateCFGDriverList(PropertyEntry drvList) throws AdapterException {

		for (String s : ClassUtil.getDriverClass()) {
			logger.info("Adding driver class [" + s + "]");
			drvList.addChoice(s, s);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#doCloseResultSet()
	 */
	public void doCloseResultSet() throws AdapterException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#onClose()
	 */
	public void onClose() {
		logger.info("Closing local data");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.IJDBCAdapter#getJdbcUrl(com.sap.hana.dp.
	 * adapter.sdk.PropertyGroup)
	 */
	public String getJdbcUrl(PropertyGroup main) throws AdapterException {
		Properties properties = new Properties();
		String jdbcUrlTemplate = null;
		String jdbcUrl = null;
		String option = "";
		Formatter fmt = new Formatter();
		try {
			properties.load(JDBCAdapter.class.getResourceAsStream("jdbc-url.properties"));
			String drvClass = main.getPropertyEntry(AdapterConstants.KEY_DRIVERCLASS).getValue();
			jdbcUrlTemplate = properties.getProperty(drvClass);

			PropertyGroup grp = main.getPropertyGroup(AdapterConstants.KEY_GROUP_CONNECTION);

			if (grp.getPropertyEntry(AdapterConstants.KEY_OPTION) != null) {
				option = grp.getPropertyEntry(AdapterConstants.KEY_OPTION).getValue();
				if (option != null && !option.isEmpty()) {

					option = ';' + option;
				}
			}

			fmt = fmt.format(jdbcUrlTemplate, grp.getPropertyEntry(AdapterConstants.KEY_HOSTNAME).getValue(),
					grp.getPropertyEntry(AdapterConstants.KEY_PORT).getValue(),
					grp.getPropertyEntry(AdapterConstants.KEY_DATABASE).getValue(), (option != null) ? option : "");
			jdbcUrl = fmt.toString();

		} catch (Exception e) {
			throw new AdapterException(e);
		} finally {
			if (properties != null) {
				properties.clear();
			}

			fmt.close();
		}
		return jdbcUrl;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter#getCapabilities(java.
	 * lang.String)
	 */
	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		List<AdapterCapability> capabilities = new ArrayList<AdapterCapability>();

		capabilities.add(AdapterCapability.CAP_BI_ADD);
		capabilities.add(AdapterCapability.CAP_BIGINT_BIND);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_GROUPBY);

		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_PROJ);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_PROJ);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_PROJ);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_WHERE);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_WHERE);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_INNER_JOIN);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_INNER_JOIN);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_INNER_JOIN);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_LEFT_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_LEFT_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_LEFT_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_FULL_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_FULL_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_FULL_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_GROUPBY);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_GROUPBY);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_GROUPBY);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_ORDERBY);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_ORDERBY);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_ORDERBY);
		capabilities.add(AdapterCapability.CAP_SELECT);
		capabilities.add(AdapterCapability.CAP_SCALAR_FUNCTIONS_NEED_ARGUMENT_CHECK);
		capabilities.add(AdapterCapability.CAP_NONEQUAL_COMPARISON);
		capabilities.add(AdapterCapability.CAP_OR_DIFFERENT_COLUMNS);
		capabilities.add(AdapterCapability.CAP_PROJECT);

		capabilities.add(AdapterCapability.CAP_LIKE);
		capabilities.add(AdapterCapability.CAP_GROUPBY);
		capabilities.add(AdapterCapability.CAP_ORDERBY);
		capabilities.add(AdapterCapability.CAP_AGGREGATES);
		capabilities.add(AdapterCapability.CAP_AGGREGATE_COLNAME);
		capabilities.add(AdapterCapability.CAP_JOINS);
		capabilities.add(AdapterCapability.CAP_JOINS_OUTER);
		capabilities.add(AdapterCapability.CAP_AND);
		capabilities.add(AdapterCapability.CAP_OR);
		capabilities.add(AdapterCapability.CAP_BETWEEN);
		capabilities.add(AdapterCapability.CAP_IN);
		capabilities.add(AdapterCapability.CAP_BI_SUBSTR);
		capabilities.add(AdapterCapability.CAP_BI_MOD);
		capabilities.add(AdapterCapability.CAP_AGGREGATES);
		capabilities.add(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);

		capbility.setCapabilities(capabilities);
		return capbility;
	}

	/*
	 * (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#rewriteSQL(java.lang.String)
	 */
	public String rewriteSQL(String sqlstatement) throws AdapterException {
		
		//String sql = SQLRewriter.rewriteSQL(sqlstatement);
		String sql = rewriter.convertSQL(sqlstatement, this.columnHelper);
		logger.info("SQL Generated [" + sql + "]");
		return sql;
	}

	
}
