/**
 * (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.crossroad.sdi.adapter.jdbc;

import java.io.FileInputStream;
import java.io.InputStream;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.AdapterConstants;
import org.crossroad.sdi.adapter.impl.ClassUtil;
import org.crossroad.sdi.adapter.impl.ISQLRewriter;
import org.crossroad.sdi.adapter.impl.RemoteSourceDescriptionFactory;
import org.crossroad.sdi.adapter.impl.SQLRewriter;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;


/**
*	JDBCAdapter Adapter.
*/
public class JDBCAdapter extends AbstractJDBCAdapter{
	private SQLRewriter sqlRewriter = new SQLRewriter(128);
	protected ExpressionBase.Type pstmtType = ExpressionBase.Type.QUERY;


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
		return main.getPropertyEntry(AdapterConstants.KEY_JDBC_URL).getValue();
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#rewriteSQL(java.lang.String)
	 */
	public String rewriteSQL(String sqlstatement) throws AdapterException {
		return this.sqlRewriter.rewriteSQL(sqlstatement);
	}

	@Override
	public Class getLoggerName() {
		return JDBCAdapter.class;
	}


	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		PropertyGroup mainGroup = RemoteSourceDescriptionFactory.getJDBCConnectionGroup();
		mainGroup.setDisplayName("JDBC Server connection definition");


	
		mainGroup.addProperty(RemoteSourceDescriptionFactory.getMappingProperty());
		
		rs.setCredentialProperties(RemoteSourceDescriptionFactory.getCredentialProperties());
		rs.setConnectionProperties(mainGroup);
		return rs;
	}

	@Override
	protected InputStream getMappingFile() throws Exception {
		PropertyGroup connectionGroup = connectionInfo.getConnectionProperties();
		boolean customMapping = (connectionGroup.getPropertyEntry(AdapterConstants.KEY_DATAMAPPING) != null)
				? AdapterConstants.BOOLEAN_TRUE.equalsIgnoreCase(
						connectionGroup.getPropertyEntry(AdapterConstants.KEY_DATAMAPPING).getValue())
				: false;

		return (customMapping
				? new FileInputStream(
						connectionGroup.getPropertyEntry(AdapterConstants.KEY_DATAMAPPING_FILE).getValue())
				: null);
	}

	@Override
	protected void postopen() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void preopen() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected ISQLRewriter getSQLRewriter() {
		return this.sqlRewriter;
	}


}
