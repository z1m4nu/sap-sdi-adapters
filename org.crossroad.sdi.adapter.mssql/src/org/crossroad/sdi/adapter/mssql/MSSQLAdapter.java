package org.crossroad.sdi.adapter.mssql;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.AdapterConstants;
import org.crossroad.sdi.adapter.impl.CapabilitiesUtils;
import org.crossroad.sdi.adapter.impl.ColumnHelper;
import org.crossroad.sdi.adapter.impl.RemoteSourceDescriptionFactory;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.ColumnCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.TableCapability;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.DataInfo;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;

/**
 * MSSQLAdapter Adapter.
 */
public class MSSQLAdapter extends AbstractJDBCAdapter {
	static Logger logger = LogManager.getLogger(MSSQLAdapter.class);
	public static final String MSSQL_JDBC_URL = "jdbc:sqlserver://%1$s:%2$s;databaseName=%3$s%4$s";
	public static final String MSSQL_JDBC_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private String schemaname_metadata = null;
	private ResultSet bulkColumnsResultSet = null;
	private PreparedStatement pstmt = null;
	protected ExpressionBase.Type pstmtType = ExpressionBase.Type.QUERY;
	private ColumnHelper columnHelper = new ColumnHelper();
	private UniqueNameTools tools = null;
	private SQLRewriter sqlRewriter = new SQLRewriter(128);

	@Override
	protected void populateCFGDriverList(PropertyEntry drvList) throws AdapterException {
		drvList.addChoice(MSSQL_JDBC_CLASS, MSSQL_JDBC_CLASS);
		drvList.setDefaultValue(MSSQL_JDBC_CLASS);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.IJDBCAdapter#getJdbcUrl(com.sap.hana.dp.
	 * adapter.sdk.PropertyGroup)
	 */
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
			fmt = fmt.format(MSSQL_JDBC_URL,
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
		logger.debug("JDBC URL ["+jdbcUrl+"]");
		return jdbcUrl;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#onClose()
	 */
	public void onClose() {
		logger.debug("Closing local data");

		closePrepareStatement();

		closeBulkResult();
	}

	private void closeBulkResult() {
		logger.debug("Closing MSSSQL ResultSet object");
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
		logger.debug("Closing MSSSQL Preparestatement object");
		if (this.pstmt != null) {
			try {
				this.pstmt.close();
			} catch (SQLException e) {
				logger.warn("Error while closing PrepareStatement", e);
			}
			this.pstmt = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.IJDBCAdapter#rewriteSQL(java.lang.String)
	 */
	public String rewriteSQL(String sqlstatement) throws AdapterException {
		logger.debug("rewriteSQL simple [" + sqlstatement + "]");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#doCloseResultSet()
	 */
	public void doCloseResultSet() throws AdapterException {
		logger.debug("In the function doCloseResultSet");

	}

	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	private BrowseNode createNodeFromResultSet() throws SQLException {
		BrowseNode node = null;
		String catalogname_metadata = this.browseResultSet.getString(1);
		if (catalogname_metadata == null) {
			catalogname_metadata = "<none>";
		}
		if ((this.listSystemData) || ((!this.listSystemData) && (!catalogname_metadata.startsWith("db_")))) {
			String displayName = null;
			String description = null;
			String uniquename = null;
			boolean expandable = false;
			boolean importable = true;
			if (this.nodeID == null) {
				displayName = catalogname_metadata;
				uniquename = catalogname_metadata;
				expandable = true;
				importable = false;
			} else {
				this.schemaname_metadata = this.browseResultSet.getString(2);
				if (this.schemaname_metadata == null) {
					this.schemaname_metadata = "<none>";
				}
				displayName = this.browseResultSet.getString(3).replaceAll("\"\\.\"", "\"\\.\\.\"");
				description = this.browseResultSet.getString(5);
				uniquename = catalogname_metadata + "." + this.schemaname_metadata + "." + displayName;
			}
			ResultSetMetaData data = this.browseResultSet.getMetaData();
			StringBuffer buffer = new StringBuffer();
			for (int col = 1; col < data.getColumnCount(); col++) {
				buffer.append("Column [");
				buffer.append(col);
				buffer.append("] - ");
				buffer.append("Name [");
				buffer.append(data.getColumnLabel(col));
				buffer.append("] - ");
				buffer.append("Value [");
				buffer.append(this.browseResultSet.getString(col));
				buffer.append("]\n");
			}

			logger.debug("createNodeFromResultSet\n" + buffer.toString());

			tools = UniqueNameTools.build(uniquename);

			node = new BrowseNode(uniquename, displayName);

			node.setImportable(importable);
			node.setExpandable(expandable);
			node.setDescription(description);
		}
		return node;
	}

	/**
	 * 
	 */
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		PropertyGroup mainGroup = RemoteSourceDescriptionFactory.getBasicJDBCConnectionGroup();
		mainGroup.setDisplayName("Microsoft SQL Server connection definition");

		PropertyEntry drvList = new PropertyEntry("jdbc.driverclass", "Driver class",
				"Select the driver class to load");
		populateCFGDriverList(drvList);
		mainGroup.addProperty(drvList);

		rs.setCredentialProperties(RemoteSourceDescriptionFactory.getCredentialProperties());
		rs.setConnectionProperties(mainGroup);
		return rs;
	}

	/**
	 * 
	 */
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		List<AdapterCapability> capabilities = new ArrayList<AdapterCapability>();

		capabilities.addAll(CapabilitiesUtils.getBICapabilities());
		capabilities.addAll(CapabilitiesUtils.getSelectCapabilities());

		capbility.setCapabilities(capabilities);
		capbility.setCapability(AdapterCapability.CAP_COLUMN_CAP);
		capbility.setCapability(AdapterCapability.CAP_TABLE_CAP);
		return capbility;
	}

	@Override
	public Metadata importMetadata(String tableuniquename) throws AdapterException {
		Metadata data = super.importMetadata(tableuniquename);
		Capabilities<TableCapability> caps = new Capabilities<TableCapability>();
		caps.setCapability(TableCapability.CAP_TABLE_AND);
		caps.setCapability(TableCapability.CAP_TABLE_AND_DIFFERENT_COLUMNS);
		caps.setCapability(TableCapability.CAP_TABLE_COLUMN_CAP);
		caps.setCapability(TableCapability.CAP_TABLE_LIMIT);
		caps.setCapability(TableCapability.CAP_TABLE_OR);
		caps.setCapability(TableCapability.CAP_TABLE_OR_DIFFERENT_COLUMNS);
		caps.setCapability(TableCapability.CAP_TABLE_SELECT);
		((TableMetadata) data).setCapabilities(caps);

		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter#
	 * updateTableMetaDataColumns(org.crossroad.sdi.adapter.impl.
	 * UniqueNameTools)
	 */
	protected List<Column> updateTableMetaDataColumns(UniqueNameTools tools) throws AdapterException {
		DatabaseMetaData meta = null;
		ResultSet rsColumns = null;

		List<Column> cols = new ArrayList<Column>();
		try {
			logger.debug("Create unique key list for [" + MSSQLAdapterUtil.SQLTableBuilder(tools) + "]");
			meta = connection.getMetaData();

			rsColumns = meta.getColumns(tools.getCatalog(), tools.getSchema(), tools.getTable(), null);

			while (rsColumns.next()) {
				String columnName = rsColumns.getString("COLUMN_NAME");
				int columnType = rsColumns.getInt("DATA_TYPE");
				String typeName = rsColumns.getString("TYPE_NAME");
				int size = rsColumns.getInt("COLUMN_SIZE");
				int nullable = rsColumns.getInt("NULLABLE");
				int scale = rsColumns.getInt("DECIMAL_DIGITS");
				String columnDesc = rsColumns.getString("REMARKS");

				int mssqlTypeId = MssqlToHanaTypeMap.getMssqlTypeId(columnType, typeName);
				DataType hanaDataType = MssqlToHanaTypeMap.getHanaDatatype(mssqlTypeId, size, true);

				Column column = new Column(columnName, hanaDataType);
				MssqlToHanaTypeMap.setDatatypeParameters(column, mssqlTypeId, size, size, scale);
				column.setDescription(columnDesc);
				column.setNullable(nullable == 1);

				columnHelper.addColumn(column, mssqlTypeId);

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

				// String Builder
				StringBuilder builder = new StringBuilder("column [");
				builder.append(columnName);
				builder.append("] - JDBC Type [");
				builder.append(columnType);
				builder.append("] - MSSQL Type [");
				builder.append(mssqlTypeId);
				builder.append("] - HANA Type [");
				builder.append(hanaDataType.name() + " (" + hanaDataType.getValue() + ")");
				builder.append("] name [");
				builder.append(typeName);
				builder.append("] Length [");
				builder.append(column.getLength());
				builder.append("] NativeLength [");
				builder.append(column.getNativeLength());
				builder.append("] Precision [");
				builder.append(column.getPrecision());
				builder.append("] NativePrecision [");
				builder.append(column.getNativePrecision());
				builder.append("]");

				logger.debug(builder.toString());

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

	/**
	 * 
	 * @param info
	 * @return
	 */
	private Map<String, Set<String>> generateSpecialTypeMap(StatementInfo info) {
		Map<String, List<Parameter>> attributes = info.getAttributes();
		Set<String> datetimeCols = new HashSet<String>();
		Set<String> smalldatetimeCols = new HashSet<String>();
		Set<String> binaryCols = new HashSet<String>();
		Set<String> varbinaryCols = new HashSet<String>();
		if (attributes != null && !attributes.isEmpty()) {
			Iterator<Entry<String, List<Parameter>>> iter = attributes.entrySet().iterator();

			while (iter.hasNext()) {
				Entry<String, List<Parameter>> entry = iter.next();
				for (int i = 0; i < ((List<Parameter>) entry.getValue()).size(); i++) {
					if (((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString()
							.startsWith("Parameter DATETIME=_dt+_")) {
						String tmp = ((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString();
						String _datetimeCols[] = tmp.split("_dt\\+_");
						for (int j = 1; j < _datetimeCols.length; j++)
							datetimeCols.add(_datetimeCols[j]);

					}
					if (((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString()
							.startsWith("Parameter SMALLDATETIME=_sdt+_")) {
						String tmp = ((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString();
						String _smalldatetimeCols[] = tmp.split("_sdt\\+_");
						for (int j = 1; j < _smalldatetimeCols.length; j++)
							smalldatetimeCols.add(_smalldatetimeCols[j]);

					}
					if (((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString()
							.startsWith("Parameter BINARY=_by+_")) {
						String tmp = ((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString();
						String _binaryCols[] = tmp.split("_by\\+_");
						for (int j = 1; j < _binaryCols.length; j++)
							binaryCols.add(_binaryCols[j]);

					}
					if (((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString()
							.startsWith("Parameter VARBINARY=_vby+_")) {
						String tmp = ((Parameter) ((List<Parameter>) entry.getValue()).get(i)).toString();
						String _varbinaryCols[] = tmp.split("_vby\\+_");
						for (int j = 1; j < _varbinaryCols.length; j++)
							varbinaryCols.add(_varbinaryCols[j]);

					}
				}

			}
		}
		Map<String, Set<String>> specialTypes = new HashMap<String, Set<String>>();
		if (datetimeCols != null)
			specialTypes.put("DATETIME", datetimeCols);
		if (smalldatetimeCols != null)
			specialTypes.put("SMALLDATETIME", smalldatetimeCols);
		if (binaryCols != null)
			specialTypes.put("BINARY", binaryCols);
		if (varbinaryCols != null)
			specialTypes.put("VARBINARY", varbinaryCols);
		return specialTypes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter#executeStatement(java.
	 * lang.String, com.sap.hana.dp.adapter.sdk.StatementInfo)
	 */
	public void executeStatement(String sqlstatement, StatementInfo info) throws AdapterException {
		Map<String, Set<String>> specialTypes = generateSpecialTypeMap(info);
		try {
			String pstmtStr = rewriteSQL(sqlstatement, specialTypes);
			logger.info("MS SQL Statement [" + pstmtStr + "]");
			info.setExecuteStatement(pstmtStr);
			this.connection.setAutoCommit(false);
			this.pstmt = this.connection.prepareStatement(pstmtStr);
			executeSelectStatement(this.pstmt, info);
		} catch (SQLException e) {
			logger.error("Error while executing statement", e);
		}
	}

	protected String rewriteSQL(String sql, Map<String, Set<String>> specialTypes) throws AdapterException {
		logger.debug("rewriteSQL [" + sql + "]");
		String rewritesqlstr = sqlRewriter.rewriteSQL(sql, specialTypes, columnHelper);

		this.pstmtType = sqlRewriter.getQueryType();
		return rewritesqlstr;
	}

	private void executeSelectStatement(PreparedStatement pstmt, StatementInfo info) throws SQLException {
		List<DataInfo> params = info.getParams();
		int paramNum = params.isEmpty() ? 0 : params.size();
		for (int i = 0; i < paramNum; i++) {
			DataInfo dataInfo = (DataInfo) params.get(i);
			String paramValue = dataInfo.getDataValue();
			switch (dataInfo.getDataType()) {
			case NCLOB:
				pstmt.setByte(i + 1, Byte.parseByte(paramValue));
				break;
			case REAL:
				pstmt.setShort(i + 1, Short.parseShort(paramValue));
				break;
			case INTEGER:
				pstmt.setInt(i + 1, Integer.parseInt(paramValue));
				break;
			case ALPHANUM:
				pstmt.setLong(i + 1, Long.parseLong(paramValue));
				break;
			case NVARCHAR:
				pstmt.setDouble(i + 1, Double.parseDouble(paramValue));
				break;
			case SMALLINT:
				pstmt.setFloat(i + 1, Float.parseFloat(paramValue));
				break;
			case VARCHAR:
				pstmt.setBigDecimal(i + 1, new BigDecimal(paramValue));
				break;
			case DATE:
				pstmt.setBytes(i + 1, paramValue.getBytes(Charset.forName("UTF-8")));
				break;
			case INVALID:
				pstmt.setDate(i + 1, Date.valueOf(paramValue));
				break;
			case TINYINT:
				pstmt.setTime(i + 1, Time.valueOf(paramValue));
				break;
			case BIGINT:
				pstmt.setTimestamp(i + 1, Timestamp.valueOf(paramValue));
				break;
			case DECIMAL:
				Blob blob = this.connection.createBlob();
				blob.setBytes(1L, paramValue.getBytes());
				pstmt.setBlob(i + 1, blob);
				break;
			case TIMESTAMP:
				Clob clob = this.connection.createClob();
				clob.setString(1L, paramValue);
				pstmt.setClob(i + 1, clob);
				break;
			case SECONDDATE:
				NClob nclob = this.connection.createNClob();
				nclob.setString(1L, paramValue);
				pstmt.setNClob(i + 1, nclob);
				break;
			case BLOB:
			case CLOB:
			case DOUBLE:
			case TIME:
			case VARBINARY:
			default:
				pstmt.setString(i + 1, paramValue);
			}
		}
		pstmt.setFetchSize(this.fetchSize);
		this.resultSet = pstmt.executeQuery();
		this.stmt = pstmt;
	}

	@Override
	public Class getLoggerName() {
		// TODO Auto-generated method stub
		return getClass();
	}

	@Override
	protected void postopen(RemoteSourceDescription arg0, boolean arg1) throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void preopen(RemoteSourceDescription arg0, boolean arg1) throws AdapterException {
		// TODO Auto-generated method stub
		
	}


}
