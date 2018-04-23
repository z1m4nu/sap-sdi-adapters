/**
 * 
 */
package org.crossroad.sdi.adapter.derby;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Formatter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.AdapterConstants;
import org.crossroad.sdi.adapter.impl.ColumnHelper;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;

/**
 * @author e.soden
 *
 */
public class DerbyAdapter extends AbstractJDBCAdapter {
	static Logger logger = LogManager.getLogger(DerbyAdapter.class);
	public static final String DB2_JDBC_URL = "jdbc:db2://%1$s:%2$s/%3$s%4$s";
	public static final String DB2_JDBC_CLASS = "com.ibm.db2.jcc.DB2Driver";
	private String schemaname_metadata = null;
	private ResultSet bulkColumnsResultSet = null;
	private PreparedStatement pstmt = null;
	protected ExpressionBase.Type pstmtType = ExpressionBase.Type.QUERY;
	private ColumnHelper columnHelper = new ColumnHelper();
	private UniqueNameTools tools = null;
	//private SQLRewriter sqlRewriter = new SQLRewriter(128);

	/**
	 * 
	 */
	public DerbyAdapter() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#getJdbcUrl(com.sap.hana.dp.adapter.sdk.PropertyGroup)
	 */
	@Override
	public String getJdbcUrl(PropertyGroup main) throws AdapterException {
		String option;
		Formatter fmt = new Formatter();
		String jdbcUrl = null;
		option = "";
		try {
			if (main.getPropertyEntry(AdapterConstants.KEY_OPTION) != null) {
				option = main.getPropertyEntry(AdapterConstants.KEY_OPTION).getValue();
				if (option != null && !option.isEmpty())
					option = (new StringBuilder(String.valueOf(';'))).append(option).toString();
			}
			fmt = fmt.format(DB2_JDBC_URL,
					new Object[] { main.getPropertyEntry(AdapterConstants.KEY_HOSTNAME).getValue(),
							main.getPropertyEntry(AdapterConstants.KEY_PORT).getValue(),
							main.getPropertyEntry(AdapterConstants.KEY_DATABASE).getValue(),
							option == null ? "" : option });
			jdbcUrl = fmt.toString();
		} catch (Exception e) {
			throw new AdapterException(e);
		} finally {
			fmt.close();

		}
		
		return jdbcUrl;
	}

	/* (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#getLoggerName()
	 */
	@Override
	public Class getLoggerName() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#onClose()
	 */
	@Override
	public void onClose() {
		logger.info("Closing local data");

		closePrepareStatement();

		closeBulkResult();
	}

	private void closeBulkResult() {
		logger.info("Closing DB2 ResultSet object");
		try {
			if (bulkColumnsResultSet != null)
				bulkColumnsResultSet.close();
		} catch (SQLException e) {
			logger.warn("Error while closing bulkColumnsResultSet connection", e);
		} finally {
			bulkColumnsResultSet = null;
		}
	}

	private void closePrepareStatement() {
		logger.info("Closing DB2 Preparestatement object");
		if (this.pstmt != null) {
			try {
				this.pstmt.close();
			} catch (SQLException e) {
				logger.warn("Error while closing PrepareStatement", e);
			}
			this.pstmt = null;
		}
	}

	/* (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#rewriteSQL(java.lang.String)
	 */
	@Override
	public String rewriteSQL(String sqlstatement) throws AdapterException {
		logger.info("In the function");
		return null;		
	}

	/* (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#doCloseResultSet()
	 */
	@Override
	public void doCloseResultSet() throws AdapterException {
		logger.info("In the function");

	}

	/* (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter#populateCFGDriverList(com.sap.hana.dp.adapter.sdk.PropertyEntry)
	 */
	@Override
	protected void populateCFGDriverList(PropertyEntry drvList) throws AdapterException {
		drvList.addChoice(DB2_JDBC_CLASS, DB2_JDBC_CLASS);
		drvList.setDefaultValue(DB2_JDBC_CLASS);
	}

}
