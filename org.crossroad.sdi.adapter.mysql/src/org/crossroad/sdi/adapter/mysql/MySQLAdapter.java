/**
 * 
 */
package org.crossroad.sdi.adapter.mysql;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.AdapterConstants;
import org.crossroad.sdi.adapter.impl.ISQLRewriter;
import org.crossroad.sdi.adapter.impl.RemoteSourceDescriptionFactory;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.ColumnCapability;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

/**
 * @author e.soden
 *
 */
public class MySQLAdapter extends AbstractJDBCAdapter {

	public static final String MYSQL_JDBC_URL = "jdbc:mysql://%1$s:%2$s/%3$s%4$s";
	public static final String MYSQL_JDBC_CLASS = "com.mysql.jdbc.Driver";

	private SQLRewriter sqlRewriter = new SQLRewriter(128);

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
					option = (new StringBuilder(String.valueOf('?'))).append(option).toString();
			}
			fmt = fmt.format(MYSQL_JDBC_URL,
					new Object[] { main.getPropertyEntry(AdapterConstants.KEY_HOSTNAME).getValue(),
							main.getPropertyEntry(AdapterConstants.KEY_PORT).getValue(),
							main.getPropertyEntry(AdapterConstants.KEY_DATABASE).getValue(),
							option == null ? "" : option });
			jdbcUrl = fmt.toString();
		} catch (Exception e) {
			logger.error("Error while generating JDBC URL", e);
			throw new AdapterException(e);
		} finally {
			fmt.close();

		}

		return jdbcUrl;
	}

	@Override
	public Class getLoggerName() {
		return getClass();
	}

	@Override
	public String rewriteSQL(String sqlstatement) throws AdapterException {
		return this.sqlRewriter.rewriteSQL(sqlstatement);
	}

	@Override
	public void doCloseResultSet() throws AdapterException {
		logger.debug("In the function doCloseResultSet");

	}

	@Override
	protected void populateCFGDriverList(PropertyEntry drvList) throws AdapterException {
		drvList.addChoice(MYSQL_JDBC_CLASS, "MySQL v5");
		drvList.addChoice("com.mysql.cj.jdbc.Driver", "MySQL v8");
		drvList.setDefaultValue(MYSQL_JDBC_CLASS);

	}

	
	protected List<Column> updateTableMetaDataColumns(UniqueNameTools tools) throws AdapterException {
		DatabaseMetaData meta = null;
		ResultSet rsColumns = null;

		List<Column> cols = new ArrayList<Column>();
		try {
			logger.debug("Create unique key list for [" + tools.getTable() + "]");
			meta = connection.getMetaData();

			rsColumns = meta.getColumns(tools.getCatalog(), tools.getSchema(), tools.getTable(), null);

			while (rsColumns.next()) {
				String columnName = rsColumns.getString("COLUMN_NAME");
				int columnType = rsColumns.getInt("DATA_TYPE");
				String typeName = rsColumns.getString("TYPE_NAME");
				int size = rsColumns.getInt("COLUMN_SIZE");
				int nullable = rsColumns.getInt("NULLABLE");
				int scale = rsColumns.getInt("DECIMAL_DIGITS");


				Column column = columnBuilder.createColumn(columnName, columnType, typeName, size, size, scale);
				if ("BIT".equalsIgnoreCase(typeName)) {
					if (size == 0) {
						column.setLength(64);
					} else {
						column.setLength(size);
					}
				}

				column.setNullable(nullable == 1);

				columnHelper.addColumn(column, columnType);
				Capabilities<ColumnCapability> columnCaps = new Capabilities<ColumnCapability>();
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_BETWEEN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_FILTER);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_GROUP);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_IN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_INNER_JOIN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_LIKE);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_NONEQUAL_COMPARISON);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_OUTER_JOIN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_SELECT);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_SORT);
				column.setCapabilities(columnCaps);

				cols.add(column);

			}

		} catch (SQLException e) {
			logger.error("Error while building column list.", e);
			throw new AdapterException(e);
		} finally {
			if (rsColumns != null) {
				try {
					rsColumns.close();
				} catch (SQLException e) {
					logger.warn("Error while closing ResultSet", e);
				}
				rsColumns = null;
			}
		}

		return cols;
	}

	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		PropertyGroup mainGroup = RemoteSourceDescriptionFactory.getBasicJDBCConnectionGroup();
		mainGroup.setDisplayName("MySQL Server connection definition");

		PropertyEntry drvList = new PropertyEntry(AdapterConstants.KEY_JDBC_DRIVERCLASS, "Driver class",
				"Select the driver class to load");
		populateCFGDriverList(drvList);
		mainGroup.addProperty(drvList);

		mainGroup.addProperty(RemoteSourceDescriptionFactory.getMappingProperty());

		rs.setCredentialProperties(RemoteSourceDescriptionFactory.getCredentialProperties());
		rs.setConnectionProperties(mainGroup);
		return rs;
	}


	@Override
	protected void postopen() throws AdapterException {

	}

	@Override
	protected void preopen() throws AdapterException {
		// TODO Auto-generated method stub

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
				: MySQLAdapter.class.getResourceAsStream("mapping.properties"));
	}

	@Override
	protected ISQLRewriter getSQLRewriter() {
		return this.sqlRewriter;
	}

}
