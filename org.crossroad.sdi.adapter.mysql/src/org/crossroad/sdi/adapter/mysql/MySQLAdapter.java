/**
 * 
 */
package org.crossroad.sdi.adapter.mysql;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.AdapterConstants;
import org.crossroad.sdi.adapter.impl.CapabilitiesUtils;
import org.crossroad.sdi.adapter.impl.RemoteSourceDescriptionFactory;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.ColumnCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.TableCapability;
import com.sap.hana.dp.adapter.sdk.AdapterException;
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
 * @author e.soden
 *
 */
public class MySQLAdapter extends AbstractJDBCAdapter {
	public static final String KEY_DATAMAPPING = "jdbc.datamapping";
	public static final String KEY_DATAMAPPING_FILE = "jdbc.datamapping.file";
	public static final String KEY_DATAMAPPING_FILE_DEFAULT = "mapping.properties";
	
	
	public static final String MYSQL_JDBC_URL = "jdbc:mysql://%1$s:%2$s/%3$s?zeroDateTimeBehavior=convertToNull%4$s";
	public static final String MYSQL_JDBC_CLASS = "com.mysql.jdbc.Driver";
	private String schemaname_metadata = null;
	private ResultSet bulkColumnsResultSet = null;
	private PreparedStatement pstmt = null;
	private SQLRewriter sqlRewriter = new SQLRewriter(128);
	protected ExpressionBase.Type pstmtType = ExpressionBase.Type.QUERY;
	
	private MySQLDataTypeMapping mapping = new  MySQLDataTypeMapping();
	
	
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException {
		// TODO Auto-generated method stub
		super.open(connectionInfo, isCDC);
		
		PropertyGroup connectionGroup = connectionInfo.getConnectionProperties();
		boolean customMapping = (connectionGroup.getPropertyEntry(KEY_DATAMAPPING) != null)
				? AdapterConstants.BOOLEAN_TRUE.equalsIgnoreCase(
						connectionGroup.getPropertyEntry(KEY_DATAMAPPING).getValue())
				: false;
		
						
		mapping.init((customMapping?connectionGroup.getPropertyEntry(KEY_DATAMAPPING_FILE).getValue():null));
	}


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
					option = (new StringBuilder(String.valueOf('&'))).append(option).toString();
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
		logger.debug("JDBC URL ["+jdbcUrl+"]");
		return jdbcUrl;
	}

	@Override
	public Class getLoggerName() {
		return getClass();
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
		logger.debug("Closing MySQL ResultSet object");
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
		logger.debug("Closing MySQL Preparestatement object");
		if (this.pstmt != null) {
			try {
				this.pstmt.close();
			} catch (SQLException e) {
				logger.warn("Error while closing PrepareStatement", e);
			}
			this.pstmt = null;
		}
	}


	@Override
	public String rewriteSQL(String sqlstatement) throws AdapterException {
		logger.debug("rewriteSQL simple [" + sqlstatement + "]");
		return null;
	}

	@Override
	public void doCloseResultSet() throws AdapterException {
		logger.debug("In the function doCloseResultSet");

	}

	@Override
	protected void populateCFGDriverList(PropertyEntry drvList) throws AdapterException {
		drvList.addChoice(MYSQL_JDBC_CLASS, MYSQL_JDBC_CLASS);
		drvList.setDefaultValue(MYSQL_JDBC_CLASS);

	}

	/**
	 * 
	 */
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> capability = new Capabilities<AdapterCapability>();
		List<AdapterCapability> capabilities = new ArrayList<AdapterCapability>();

		capabilities.addAll(CapabilitiesUtils.getBICapabilities());
		capabilities.addAll(CapabilitiesUtils.getSelectCapabilities());

		capability.setCapabilities(capabilities);
		capability.setCapability(AdapterCapability.CAP_COLUMN_CAP);
		capability.setCapability(AdapterCapability.CAP_TABLE_CAP);
		
		capability.setCapability(AdapterCapability.CAP_SELECT);
		capability.setCapability(AdapterCapability.CAP_AND);
		capability.setCapability(AdapterCapability.CAP_PROJECT);
		capability.setCapability(AdapterCapability.CAP_LIMIT);
		capability.setCapability(AdapterCapability.CAP_LIMIT_ARG);
		capability.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
		capability.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		capability.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE);
		capability.setCapability(AdapterCapability.CAP_WHERE);
		capability.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capability.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capability.setCapability(AdapterCapability.CAP_LIKE);
		capability.setCapability(AdapterCapability.CAP_NONEQUAL_COMPARISON);
		capability.setCapability(AdapterCapability.CAP_AGGREGATES);
		
		return capability;
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
				String columnDesc = rsColumns.getString("REMARKS");
				
				//Column column = MySQLHANAType.buildMySQLColumn(columnName, columnType, typeName, size, size, scale);
				Column column = mapping.createColumn(columnName, columnType, typeName, size, size, scale);
				column.setDescription(columnDesc);
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


		PropertyEntry drvList = new PropertyEntry("jdbc.driverclass", "Driver class",
				"Select the driver class to load");
		populateCFGDriverList(drvList);
		mainGroup.addProperty(drvList);

		
		PropertyEntry mappingDataChoice = new PropertyEntry(KEY_DATAMAPPING, "Use custom data mapping",
				"", false);
		mappingDataChoice.addChoice(AdapterConstants.BOOLEAN_TRUE, AdapterConstants.BOOLEAN_TRUE);
		mappingDataChoice.addChoice(AdapterConstants.BOOLEAN_FALSE, AdapterConstants.BOOLEAN_FALSE);
		mappingDataChoice.setDefaultValue(AdapterConstants.BOOLEAN_FALSE);
		
		PropertyEntry mappingFile = new PropertyEntry(KEY_DATAMAPPING_FILE, "Mapping file", "Mapping file", false);
		mappingFile.setDefaultValue(KEY_DATAMAPPING_FILE);
		
		mainGroup.addProperty(mappingDataChoice);
		mainGroup.addProperty(mappingFile);
		
		rs.setCredentialProperties(RemoteSourceDescriptionFactory.getCredentialProperties());
		rs.setConnectionProperties(mainGroup);
		return rs;
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
			logger.info("MySQL Statement [" + pstmtStr + "]");
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


}
