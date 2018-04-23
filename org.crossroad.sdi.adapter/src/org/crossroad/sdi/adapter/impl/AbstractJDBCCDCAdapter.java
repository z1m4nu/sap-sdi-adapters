/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterCDC;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.LobCharset;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterStatistics;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CallableProcedure;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.DataDictionary;
import com.sap.hana.dp.adapter.sdk.FunctionMetadata;
import com.sap.hana.dp.adapter.sdk.Index;
import com.sap.hana.dp.adapter.sdk.LatencyTicketSpecification;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.ParametersResponse;
import com.sap.hana.dp.adapter.sdk.ProcedureMetadata;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.ReceiverConnection;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.Timestamp;
import com.sap.hana.dp.adapter.sdk.UniqueKey;

/**
 * @author e.soden
 *
 */
public abstract class AbstractJDBCCDCAdapter extends AdapterCDC implements IJDBCAdapter {
	public static Logger logger = null;
	/** JDBC connections **/
	protected Connection connection = null;
	protected Statement stmt = null;
	protected ResultSet resultSet = null;
	protected ResultSet browseResultSet = null;

	/** Node user is browsing **/
	protected String nodeID = null;
	/** Browse node offset **/
	protected int browseOffset = 0;
	protected int fetchSize;
	protected boolean listSystemData = false;
	protected boolean nullableAsEmpty = false;

	protected HashMap<Long, InputStream> blobHandle;
	protected HashMap<Long, Reader> clobHandle;

	/**
	 * 
	 */
	public AbstractJDBCCDCAdapter() {
		logger = LogManager.getLogger(getLoggerName());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.AdapterCDC#addSubscription(com.sap.hana.dp.
	 * adapter.sdk.SubscriptionSpecification)
	 */
	@Override
	public String addSubscription(SubscriptionSpecification arg0) throws AdapterException {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.AdapterCDC#beginMarker(java.lang.String,
	 * com.sap.hana.dp.adapter.sdk.SubscriptionSpecification)
	 */
	@Override
	public void beginMarker(String arg0, SubscriptionSpecification arg1) throws AdapterException {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.AdapterCDC#committedChange(com.sap.hana.dp.
	 * adapter.sdk.SubscriptionSpecification)
	 */
	@Override
	public void committedChange(SubscriptionSpecification arg0) throws AdapterException {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.AdapterCDC#endMarker(java.lang.String,
	 * com.sap.hana.dp.adapter.sdk.SubscriptionSpecification)
	 */
	@Override
	public void endMarker(String arg0, SubscriptionSpecification arg1) throws AdapterException {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.AdapterCDC#getAdapterStatistics()
	 */
	@Override
	public AdapterStatistics getAdapterStatistics() {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.AdapterCDC#getCDCCapabilities(java.lang.
	 * String)
	 */
	@Override
	public Capabilities<AdapterCapability> getCDCCapabilities(String arg0) throws AdapterException {
		Capabilities<AdapterCapability> capabilities = getCapabilities(arg0);
		capabilities.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
		return capabilities;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.AdapterCDC#removeSubscription(com.sap.hana.dp
	 * .adapter.sdk.SubscriptionSpecification)
	 */
	@Override
	public void removeSubscription(SubscriptionSpecification arg0) throws AdapterException {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.AdapterCDC#requireDurableMessaging()
	 */
	@Override
	public boolean requireDurableMessaging() {
		logger.debug("In the function");
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.AdapterCDC#setAdapterStatisticsUpdateInterval
	 * (int)
	 */
	@Override
	public void setAdapterStatisticsUpdateInterval(int arg0) {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.AdapterCDC#start(com.sap.hana.dp.adapter.sdk.
	 * ReceiverConnection,
	 * com.sap.hana.dp.adapter.sdk.SubscriptionSpecification)
	 */
	@Override
	public void start(ReceiverConnection arg0, SubscriptionSpecification arg1) throws AdapterException {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.AdapterCDC#startLatencyTicket(com.sap.hana.dp
	 * .adapter.sdk.LatencyTicketSpecification)
	 */
	@Override
	public void startLatencyTicket(LatencyTicketSpecification arg0) throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.AdapterCDC#stop(com.sap.hana.dp.adapter.sdk.
	 * SubscriptionSpecification)
	 */
	@Override
	public void stop(SubscriptionSpecification arg0) throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.AdapterCDC#supportsRecovery()
	 */
	@Override
	public boolean supportsRecovery() {
		logger.debug("In the function");
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#beginTransaction()
	 */
	@Override
	public void beginTransaction() throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#browseMetadata()
	 */
	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		/*
		 * The call sequence is setBrowseNodeId(uniquename) and then multiple
		 * calls of browseMetadata() to return one page after the other of
		 * nodes.
		 */
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		try {
			if (this.nodeID == null) {
				/**
				 * This is a root node, we want to be expandable and not
				 * importable
				 **/
				if (browseOffset == 0) {
					browseResultSet = connection.getMetaData().getCatalogs();
				} else if (browseResultSet == null) {
					/*
					 * This should actually never happen. When the browseOffset
					 * != 0 the resultset should be open still.
					 */
					return null;
				}
				while (browseResultSet.next()) {
					browseOffset++;
					String catalogname = browseResultSet.getString(1);
					BrowseNode node = new BrowseNode(catalogname, catalogname);
					node.setImportable(false);
					/*
					 * we do not allow to expand a catalog which has a dot in
					 * its name as that breaks the unique name format
					 */
					node.setExpandable(catalogname.indexOf('.') == -1);
					nodes.add(node);
					if (browseOffset % fetchSize == fetchSize - 1)
						break;
				}
				if (browseOffset == 0) {
					// This jdbc source has no catalogs hence a default is to be
					// used
					browseOffset++;
					String catalogname = "<none>";
					BrowseNode node = new BrowseNode(catalogname, catalogname);
					node.setImportable(false);
					node.setExpandable(true);
					nodes.add(node);
				} else if (browseOffset % fetchSize != fetchSize - 1) {
					/*
					 * We did exit above loop before reaching the fetch size,
					 * seems there is no more data. Hence the browseResultSet
					 * can be closed
					 */
					browseResultSet.close();
					browseResultSet = null;
				}
			} else {
				String[] nodecomponents = this.nodeID.split("\\.");

				String catalogname = nodecomponents[0];
				String catalog_search_string;
				if (catalogname.equals("<none>")) {
					catalog_search_string = null; // This means we return all
													// schemas without a catalog
													// whereas NULL would mean
													// all schemas of all
													// catalogs
				} else {
					catalog_search_string = catalogname;
				}

				if (nodecomponents.length == 1) {
					// the catalog node got expanded

					if (browseOffset == 0) {
						// get all Schemas of the current catalog
						browseResultSet = connection.getMetaData().getSchemas(catalog_search_string, null);
					} else if (browseResultSet == null) {
						return null;
					}
					while (browseResultSet.next()) {
						browseOffset++;
						String schemaname = browseResultSet.getString(1);

						// catalogname should be the same as requested but
						// better play it safe
						String catalogname_metadata = browseResultSet.getString(2);
						if (catalogname_metadata == null) {
							catalogname_metadata = AdapterConstants.NULL_AS_STRING;
						}
						String uniquename = catalogname_metadata + "." + schemaname;
						BrowseNode node = new BrowseNode(uniquename, schemaname);
						node.setImportable(false);
						/*
						 * we do not allow to expand a schema which has a dot in
						 * its name as that breaks the unique name format
						 */
						node.setExpandable(schemaname.indexOf('.') == -1);
						nodes.add(node);
						if (browseOffset % fetchSize == fetchSize - 1)
							break;
					}
					if (browseOffset == 0) {
						// This jdbc source has no catalogs hence a default is
						// to be used
						browseOffset++;
						String schemaname = AdapterConstants.NULL_AS_STRING;
						String uniquename = catalogname + "." + schemaname;
						BrowseNode node = new BrowseNode(uniquename, schemaname);
						node.setImportable(false);
						node.setExpandable(true);
						nodes.add(node);
					} else if (browseOffset % fetchSize != fetchSize - 1) {
						/*
						 * We did exit above loop before reaching the fetch
						 * size, seems there is no more data. Hence the
						 * browseResultSet can be closed
						 */
						browseResultSet.close();
						browseResultSet = null;
					}

				} else {
					// the nodeid is two levels deep: catalog.schema

					String schemaname = nodecomponents[1];
					String schema_search_string;
					if (schemaname.equals(AdapterConstants.NULL_AS_STRING)) {
						schema_search_string = null; // This means we return all
														// schemas without a
														// catalog whereas NULL
														// would mean all
														// schemas of all
														// catalogs
					} else {
						schema_search_string = schemaname;
					}

					if (browseOffset == 0) {
						// get all Schemas of the current catalog
						browseResultSet = connection.getMetaData().getTables(catalog_search_string,
								schema_search_string, "%",
								/*
								 * new String[] { "TABLE", "VIEW",
								 * "SYSTEM TABLE" }
								 */null);
					}

					while (browseResultSet.next()) {
						browseOffset++;

						String catalogname_metadata = browseResultSet.getString(1);
						if (catalogname_metadata == null) {
							catalogname_metadata = "<none>";
						}
						String schemaname_metadata = browseResultSet.getString(2);
						if (schemaname_metadata == null) {
							schemaname_metadata = "<none>";
						}

						String tablename = browseResultSet.getString(3);
						String description = browseResultSet.getString(5);

						String tableType = browseResultSet.getString(4);

						String uniquename = catalogname_metadata + "." + schemaname_metadata + "." + tablename;
						BrowseNode node = new BrowseNode(uniquename, tablename);
						node.setDescription(description);
						node.setNodeType(AdapterUtil.strToNodeType(tableType));

						/*
						 * Tablenames with a dot character in them break the
						 * unique name format, hence we cannot deal with those.
						 * We show them but do not allow to import them.
						 */
						node.setImportable(tablename.indexOf('.') == -1);
						node.setExpandable(false);
						nodes.add(node);
						if (browseOffset % fetchSize == fetchSize - 1)
							break;
					}
					if (browseOffset % fetchSize != fetchSize - 1) {
						/*
						 * We did exit above loop before reaching the fetch
						 * size, seems there is no more data. Hence the
						 * browseResultSet can be closed
						 */
						browseResultSet.close();
						browseResultSet = null;
					}
				}
			}
			return nodes;
		} catch (SQLException e) {
			throw new AdapterException(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#close()
	 */
	@Override
	public void close() throws AdapterException {

		logger.info("Close connections and resultSet");

		/**
		 * Cleanup connections, thread and all the element your adapter is
		 * using.
		 * 
		 */
		try {

			onClose();
			/*
			 * Close resultSet
			 */
			closeLocalResultSet();

			closeLocalBrowseResultSet();

			closeLocalStatement();

			closeLocalConnection();

		} finally {
			resultSet = null;
			browseResultSet = null;
			connection = null;

			blobHandle.clear();
			clobHandle.clear();
		}
	}

	/**
	 * 
	 */
	protected void closeLocalResultSet() {
		if (resultSet != null) {
			logger.info("Closing resultset....");
			try {
				resultSet.close();
			} catch (SQLException e) {
				logger.warn("Issues when closing resultSet", e);
			} finally {
				resultSet = null;
			}
		}
	}

	protected void closeLocalBrowseResultSet() {
		if (browseResultSet != null) {
			logger.info("Closing browse resultset....");
			try {
				browseResultSet.close();
			} catch (SQLException e) {
				logger.warn("Issues when closing browseResultSet", e);
			} finally {
				browseResultSet = null;
			}
		}
	}

	protected void closeLocalStatement() {

		if (stmt != null) {
			logger.info("Closing Statement....");
			try {
				stmt.close();
			} catch (SQLException e) {
				logger.warn("Issues when closing browseResultSet", e);
			} finally {
				stmt = null;
			}
		}
	}

	private void closeLocalConnection() {

		if (connection != null) {
			logger.info("Closing Connection....");
			try {
				connection.close();
			} catch (SQLException e) {
				logger.warn("Issues when closing connection", e);
			} finally {
				connection = null;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#closeResultSet()
	 */
	@Override
	public void closeResultSet() throws AdapterException {
		logger.info("Ask to close resulSet");

		doCloseResultSet();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#commitTransaction()
	 */
	@Override
	public void commitTransaction() throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#executeCall(com.sap.hana.dp.adapter.
	 * sdk.FunctionMetadata)
	 */
	@Override
	public void executeCall(FunctionMetadata arg0) throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#executePreparedInsert(java.lang.
	 * String, com.sap.hana.dp.adapter.sdk.StatementInfo)
	 */
	@Override
	public void executePreparedInsert(String arg0, StatementInfo arg1) throws AdapterException {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#executePreparedUpdate(java.lang.
	 * String, com.sap.hana.dp.adapter.sdk.StatementInfo)
	 */
	@Override
	public void executePreparedUpdate(String arg0, StatementInfo arg1) throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#executeStatement(java.lang.String,
	 * com.sap.hana.dp.adapter.sdk.StatementInfo)
	 */
	@Override
	public void executeStatement(String arg0, StatementInfo arg1) throws AdapterException {
		/**
		 * Since we have a simple adapter with no push down. So we will directly
		 * use it. If you do support pushdown you need to parse the sql, figure
		 * out the columns being returned and save a copy to be used in getNext.
		 */

		blobHandle.clear();
		clobHandle.clear();

		String sourcesql = rewriteSQL(arg0);

		try {
			connection.setAutoCommit(false);
			stmt.setFetchSize(fetchSize);// So that fetch size work.
			logger.trace(sourcesql);
			resultSet = stmt.executeQuery(sourcesql);
		} catch (SQLException e) {
			throw new AdapterException(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#executeUpdate(java.lang.String,
	 * com.sap.hana.dp.adapter.sdk.StatementInfo)
	 */
	@Override
	public int executeUpdate(String arg0, StatementInfo arg1) throws AdapterException {
		logger.debug("In the function");
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#getCapabilities(java.lang.String)
	 */
	@Override
	public Capabilities<AdapterCapability> getCapabilities(String arg0) throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		List<AdapterCapability> capabilities = new ArrayList<AdapterCapability>();
		if ((System.getenv("DP_AGENT_DIR") == null) || (System.getenv("CAPS_INI") == null)) {
			capabilities.add(AdapterCapability.CAP_ALTER_TAB_WITH_ADD);
			capabilities.add(AdapterCapability.CAP_ALTER_TAB_WITH_DROP);
			capabilities.add(AdapterCapability.CAP_WINDOWING_FUNC);
			capabilities.add(AdapterCapability.CAP_BI_ADD);
			capabilities.add(AdapterCapability.CAP_BIGINT_BIND);
			capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_GROUPBY);
			capabilities.add(AdapterCapability.CAP_INSERT_SELECT_ORDERBY);
			capabilities.add(AdapterCapability.CAP_DELETE);

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

			capabilities.add(AdapterCapability.CAP_LIMIT);
			capabilities.add(AdapterCapability.CAP_TRANSACTIONAL_CDC);

		} else {
			String sFileName = System.getenv("DP_AGENT_DIR") + "/configuration/" + System.getenv("CAPS_INI");
			try {
				Properties prop = new Properties();
				InputStream input = null;
				input = new FileInputStream(sFileName);
				prop.load(input);

				Enumeration<?> e = prop.propertyNames();
				while (e.hasMoreElements()) {
					String key = (String) e.nextElement();
					int value = Integer.parseInt(prop.getProperty(key));
					capabilities.add(AdapterCapability.valueOf(value));
				}
			} catch (IOException e) {
				throw new AdapterException(e, e.getLocalizedMessage());
			}
		}

		capbility.setCapabilities(capabilities);
		return capbility;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#getLob(long, byte[], int)
	 */
	@Override
	public int getLob(long lobHandleId, byte[] bytes, int bufferSize) throws AdapterException {
		try {
			int readBytes;
			if (blobHandle.containsKey(lobHandleId)) {
				InputStream is = blobHandle.get(lobHandleId);
				readBytes = is.read(bytes); // is.read(bytes, offSet,
											// bufferSize);
				if (readBytes < 0)
					return 0; // We can not send 0 bytes array
				return readBytes;
			} else if (clobHandle.containsKey(lobHandleId)) {
				Reader is = clobHandle.get(lobHandleId);
				char[] buffer = new char[bufferSize];
				readBytes = is.read(buffer, 0, bufferSize);
				if (readBytes < 0)
					return 0; // We can not send 0 bytes array
				ByteBuffer bb = ByteBuffer.wrap(bytes);
				CharBuffer cb = CharBuffer.wrap(buffer, 0, readBytes);
				ByteBuffer result = Charset.forName("UTF-8").encode(cb);
				bb.put(result);
				return result.position();
			} else {
				return 0;
			}
		} catch (IOException e) {
			throw new AdapterException(e.getLocalizedMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#getMetadataDetail(java.lang.String)
	 */
	@Override
	public Metadata getMetadataDetail(String arg0) throws AdapterException {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#getNext(com.sap.hana.dp.adapter.sdk.
	 * AdapterRowSet)
	 */
	@Override
	public void getNext(AdapterRowSet rowList) throws AdapterException {
		try {
			// blobHandle.clear();
			/*
			 * This method will be called multiple times based on what you
			 * return. Make sure you honor the fetchSize requirement by only
			 * sending that many rows.
			 */
			int rowNum = 0;
			while (resultSet.next()) {
				/*
				 * For each row in resultSet, create a AdapterRow Set the values
				 * for each columns
				 */
				rowList.newRow();
				List<Column> columns = rowList.getColumns();
				for (int i = 0; i < columns.size(); i++) {
					// Always add to last row
					setValue(rowList.getRow(rowList.getRowCount() - 1), columns.get(i), resultSet, i, rowNum);
					/**
					 * Note in case of lob datatypes, if you put lob columns
					 * here let's say you send 10 rows with lob id inside.
					 * Framework will call getLob to get all the lob data before
					 * fetching the next batch of rows using getNext.
					 */
				}
				rowNum++;
				if (rowNum == fetchSize) // Do not add more than fetchSize.
					break;
			}

		} catch (SQLException e) {
			throw new AdapterException(e.getMessage());
		}
	}

	/**
	 * Helper Method Call the appropriate method on the row the set the column
	 * value. You can extend it to other datatypes.
	 */
	private void setValue(AdapterRow row, Column column, ResultSet rs, int colIndex, int rowIndex)
			throws AdapterException, SQLException {

		Calendar cal = Calendar.getInstance();
		Long lobId = (long) (rowIndex * this.fetchSize + colIndex);
		String str = null;
		switch (column.getDataType()) {
		case TINYINT:
			row.setColumnValue(colIndex, rs.getDouble(colIndex + 1));
			break;
		case INTEGER:
			row.setColumnValue(colIndex, rs.getInt(colIndex + 1));
			break;
		case BIGINT:
			row.setColumnValue(colIndex, rs.getLong(colIndex + 1));
			break;
		case DOUBLE:
			row.setColumnValue(colIndex, rs.getDouble(colIndex + 1));
			break;
		case DECIMAL:
			BigDecimal bigDecimal = rs.getBigDecimal(colIndex + 1);
			if (bigDecimal == null) {
				row.setColumnNull(colIndex);
				break;
			}
			row.setColumnValue(colIndex, bigDecimal);
			break;
		case VARBINARY:
			byte[] bytes = rs.getBytes(colIndex + 1);
			if (bytes == null) {
				row.setColumnNull(colIndex);
				break;
			}
			row.setColumnValue(colIndex, bytes);
			break;
		case DATE:
			Date date = rs.getDate(colIndex + 1);
			if (date == null) {
				row.setColumnNull(colIndex);
				break;
			}
			cal.setTime(date);
			row.setColumnValue(colIndex, new Timestamp(cal));
			break;
		case SECONDDATE:
		case TIME:
			Time time = rs.getTime(colIndex + 1);
			if (time == null) {
				row.setColumnNull(colIndex);
				break;
			}
			cal.setTime(time);
			row.setColumnValue(colIndex, new Timestamp(cal));
			break;
		case TIMESTAMP:
			java.sql.Timestamp timeStamp = rs.getTimestamp(colIndex + 1);
			if (timeStamp == null) {
				row.setColumnNull(colIndex);
				break;
			}
			cal.setTimeInMillis(timeStamp.getTime());
			row.setColumnValue(colIndex, new Timestamp(cal));
			break;
		case BLOB:
			Blob blob1 = rs.getBlob(colIndex + 1);
			if (blob1 == null) {
				row.setColumnLobIdValue(colIndex, 0, LobCharset.ASCII);
				column.setNullable(true); /// TODO
				break;
			}
			blobHandle.put(lobId, blob1.getBinaryStream());
			row.setColumnLobIdValue(colIndex, lobId, LobCharset.ASCII);
			break;
		case CLOB:
		case NCLOB:
			Clob clob = rs.getClob(colIndex + 1);
			if (clob == null) {
				row.setColumnLobIdValue(colIndex, 0, LobCharset.ASCII);
				column.setNullable(true); /// TODO
			} else {
				clobHandle.put(lobId, clob.getCharacterStream());// clob.getAsciiStream());
				row.setColumnLobIdValue(colIndex, lobId, LobCharset.UTF_8);
			}
			break;
		case VARCHAR:
		case NVARCHAR:
			str = rs.getString(colIndex + 1);
			if (str == null) {
				str = (nullableAsEmpty) ? "" : null;
				row.setColumnNull(colIndex);
			}			
			row.setColumnValue(colIndex, str);
			break;
		default:
			logger.info("Unknown Type " + column.getDataType() + " for column "
					+ rs.getMetaData().getColumnName(colIndex + 1));
			str = rs.getString(colIndex + 1);
			if (str == null) {
				str = (nullableAsEmpty) ? "" : null;
				row.setColumnNull(colIndex);
			}
			row.setColumnValue(colIndex, str);
			break;
		}
	}

	@Override
	public Metadata importMetadata(String tableuniquename) throws AdapterException {
		/*
		 * nodeId does match the format: catalog.schema.tablename
		 */
		UniqueNameTools tools = UniqueNameTools.build(tableuniquename);

		if ((tools.getCatalog() == null && tools.getSchema() == null)) {
			throw new AdapterException(
					"Unique Name of the table does not match the format catalog.schema.tablename: " + tableuniquename);
		}

		if (tools.getTable() == null) {
			throw new AdapterException("Table Name portion cannot be empty: " + tableuniquename);
		}

		TableMetadata metas = new TableMetadata();
		metas.setName(tools.getUniqueName());
		metas.setPhysicalName(tools.getTable());
		metas.setColumns(updateTableMetaDataColumns(tools));
		metas.setUniqueKeys(updateTableMetaDataUniqueKeys(tools));
		metas.setIndices(updateTableMetaDataIndices(tools));
		setPrimaryFlagForColumns(metas);

		return metas;
	}

	/**
	 * 
	 * @param tableuniquename
	 * @return
	 * @throws AdapterException
	 */
	protected List<UniqueKey> updateTableMetaDataUniqueKeys(UniqueNameTools tools) throws AdapterException {
		ArrayList<UniqueKey> uniqueKeys = new ArrayList<UniqueKey>();
		DatabaseMetaData meta = null;
		ResultSet rs = null;
		HashMap<String, List<String>> map = new HashMap<String, List<String>>();

		try {
			logger.info("Create unique key list for [" + tools.getCatalog() + "." + tools.getSchema() + "."
					+ tools.getTable() + "]");
			meta = connection.getMetaData();

			rs = meta.getPrimaryKeys(tools.getCatalog(), tools.getSchema(), tools.getTable());

			while (rs.next()) {
				String indexName = rs.getString("PK_NAME");
				if (indexName == null)
					continue;
				String fieldName = rs.getString("COLUMN_NAME");
				if (!map.containsKey(indexName))
					map.put(indexName, new ArrayList<String>());
				map.get(indexName).add(fieldName);
			}
			for (String key : map.keySet()) {
				UniqueKey uniqueKey = new UniqueKey(key, map.get(key));
				uniqueKey.setPrimary(true);
				uniqueKeys.add(uniqueKey);
			}
		} catch (SQLException e) {
			logger.error("Error while creating key list", e);
			throw new AdapterException(e);
		} finally {
			logger.info("Closing ResultSet...");
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				logger.warn("Failed to close ResultSet", e);
			}

			rs = null;
		}
		return uniqueKeys;
	}

	/**
	 * 
	 * @param nodecomponent
	 * @param tableMetadata
	 * @throws AdapterException
	 */
	protected void updateTableMetaData(Properties nodecomponent, TableMetadata tableMetadata) throws AdapterException {
	}

	/**
	 * 
	 * @param catalog
	 * @param schema
	 * @param tableName
	 * @param columnNamePattern
	 * @return
	 * @throws AdapterException
	 */
	protected List<Column> updateTableMetaDataColumns(UniqueNameTools tools) throws AdapterException {
		ResultSet rsColumns = null;
		DatabaseMetaData meta = null;
		List<Column> cols = new ArrayList<Column>();

		try {
			meta = connection.getMetaData();
			// catalog, schemaPattern, tableNamePatter, types
			rsColumns = meta.getColumns(tools.getCatalog(), tools.getSchema(), tools.getTable(), null);
			while (rsColumns.next()) {
				String columnName = rsColumns.getString("COLUMN_NAME");
				int columnType = rsColumns.getInt("DATA_TYPE");
				String typeName = rsColumns.getString("TYPE_NAME");

				if (typeName.compareTo("date") == 0)
					columnType = 91;
				else if (typeName.compareTo("time") == 0)
					columnType = 92;
				else if (typeName.compareTo("datetime2") == 0)
					columnType = 93;
				else if (typeName.compareTo("text") == 0)
					columnType = java.sql.Types.CLOB;
				else if (typeName.compareTo("ntext") == 0)
					columnType = java.sql.Types.NCLOB;
				else if (typeName.compareTo("image") == 0)
					columnType = java.sql.Types.BLOB;
				else if (typeName.compareTo("nvarchar") == 0)
					columnType = java.sql.Types.NVARCHAR;
				else if (typeName.compareTo("nchar") == 0)
					columnType = java.sql.Types.NCHAR;
				// @Bug Index Server Changes pending. FIXME uncomment when IS is
				// fixed
				else if (typeName.compareTo("smalldatetime") == 0)
					// columnType = -100; //Since java does not have this, we
					// create our
					// own version.
					columnType = java.sql.Types.TIMESTAMP;

				int size = rsColumns.getInt("COLUMN_SIZE");
				int nullable = rsColumns.getInt("NULLABLE");

				Column col = new Column(columnName, getAdapterDataType(columnType));
				col.setLength(size);
				col.setNullable(nullable == DatabaseMetaData.columnNullable);
				col.setNativeDataType(typeName);
				if (getAdapterDataType(columnType) == DataType.DECIMAL) {
					col.setPrecision(size);
					col.setScale(rsColumns.getInt("DECIMAL_DIGITS"));
				}

				logger.info(AdapterUtil.dumpResultSet(rsColumns));

				cols.add(col);
			}

		} catch (SQLException e) {
			logger.error("Error while building columns", e);
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
	 * @param nodecomponents
	 * @return
	 * @throws AdapterException
	 */
	protected List<Index> updateTableMetaDataIndices(UniqueNameTools tools) throws AdapterException {
		List<Index> indices = new ArrayList<Index>();
		ResultSet rs = null;

		try {
			rs = connection.getMetaData().getIndexInfo(tools.getCatalog(), tools.getSchema(), tools.getTable(), false,
					true);

			HashMap<String, Index> map = new HashMap<String, Index>();
			while (rs.next()) {
				String indexname = rs.getString("INDEX_NAME");

				if (!map.containsKey(indexname)) {
					Index index = new Index(indexname);
					List<String> columns = new ArrayList<String>();
					columns.add(rs.getString("COLUMN_NAME"));
					index.setColumnNames(columns);
					map.put(indexname, index);
				} else {
					Index index = map.get(indexname);
					index.getColumnNames().add(rs.getString("COLUMN_NAME"));
				}
			}

			for (Index index : map.values()) {
				indices.add(index);
			}

		} catch (SQLException e) {
			logger.error("Error while creating index list", e);
			throw new AdapterException(e);
		} finally {

		}

		return indices;
	}

	private void setPrimaryFlagForColumns(TableMetadata metas) {
		List<Column> columns = metas.getColumns();
		List<UniqueKey> keys = metas.getUniqueKeys();
		for (UniqueKey key : keys) {
			List<String> columnNames = key.getColumnNames();
			for (Column column : columns)
				if (columnNames.contains(column.getName()))
					column.setPrimaryKey(true);
		}
	}

	/**
	 * 
	 * @param dbType
	 * @return
	 * @throws AdapterException
	 */
	public DataType getAdapterDataType(int dbType) throws AdapterException {
		switch (dbType) {
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			return DataType.VARCHAR;

		case java.sql.Types.NCHAR:
		case java.sql.Types.NVARCHAR:
			return DataType.NVARCHAR;

		case java.sql.Types.INTEGER:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.TINYINT:
			return DataType.INTEGER;

		case java.sql.Types.BIGINT:
			return DataType.BIGINT;

		case java.sql.Types.NUMERIC:
		case java.sql.Types.DECIMAL:
			return DataType.DECIMAL;

		case java.sql.Types.REAL:
		case java.sql.Types.FLOAT:
			return DataType.REAL;

		case java.sql.Types.DOUBLE:
			return DataType.DOUBLE;

		case java.sql.Types.TIMESTAMP:
			return DataType.TIMESTAMP;

		case java.sql.Types.DATE:
			return DataType.DATE;

		case java.sql.Types.TIME:
			return DataType.TIME;

		case java.sql.Types.BLOB:
			return DataType.BLOB;

		case java.sql.Types.LONGVARCHAR:
		case java.sql.Types.CLOB:
			return DataType.CLOB;

		case java.sql.Types.LONGNVARCHAR:
		case java.sql.Types.NCLOB:
			return DataType.NCLOB;

		case java.sql.Types.BINARY:
			return DataType.VARBINARY;
		case -100:
			return DataType.SECONDDATE;
		default:
			logger.warn("DB TYPE [" + dbType + "] is not supported will be translated as VARCHAR");
			return DataType.VARCHAR;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#getRemoteSourceDescription()
	 */
	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		logger.info("Retrieve remote source description");
		RemoteSourceDescription rs = new RemoteSourceDescription();
		PropertyGroup mainGroup = new PropertyGroup(AdapterConstants.KEY_GROUP_MAIN, "JBDC Configuration");

		PropertyEntry entry = new PropertyEntry(AdapterConstants.KEY_THIRDPARTY, "Use thirdparty JDBC", "Expert mode");
		entry.addChoice(AdapterConstants.BOOLEAN_TRUE, AdapterConstants.BOOLEAN_TRUE);
		entry.addChoice(AdapterConstants.BOOLEAN_FALSE, AdapterConstants.BOOLEAN_FALSE);
		entry.setDefaultValue(AdapterConstants.BOOLEAN_FALSE);

		/*
		 * Driver list
		 */
		PropertyEntry drvList = new PropertyEntry(AdapterConstants.KEY_DRIVERCLASS, "Driver class",
				"Select the driver class to load");
		drvList.addDependency(entry, AdapterConstants.BOOLEAN_FALSE);

		/*
		 * Expert mode configuration
		 */
		PropertyGroup expertGroup = RemoteSourceDescriptionFactory.getExpertGroup();

		entry.getPropertyDependencies().put(expertGroup, AdapterConstants.BOOLEAN_TRUE);
		mainGroup.addProperty(expertGroup);

		/*
		 * Add to main group
		 */
		mainGroup.addProperty(entry);
		mainGroup.addProperty(drvList);
		mainGroup.addProperty(RemoteSourceDescriptionFactory.getBasicJDBCConnectionGroup());
		/*
		 * Deciding of the load
		 */

		rs.setCredentialProperties(RemoteSourceDescriptionFactory.getCredentialProperties());
		rs.setConnectionProperties(mainGroup);

		return rs;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#getSourceVersion(com.sap.hana.dp.
	 * adapter.sdk.RemoteSourceDescription)
	 */
	@Override
	public String getSourceVersion(RemoteSourceDescription arg0) throws AdapterException {
		return "0.0.0";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#importMetadata(java.lang.String,
	 * java.util.List)
	 */
	@Override
	public Metadata importMetadata(String arg0, List<Parameter> arg1) throws AdapterException {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#loadColumnsDictionary()
	 */
	@Override
	public DataDictionary loadColumnsDictionary() throws AdapterException {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#loadTableDictionary(java.lang.String)
	 */
	@Override
	public List<BrowseNode> loadTableDictionary(String arg0) throws AdapterException {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#open(com.sap.hana.dp.adapter.sdk.
	 * RemoteSourceDescription, boolean)
	 */
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean arg1) throws AdapterException {
		String username = "";
		String password = "";
		CredentialProperties p = connectionInfo.getCredentialProperties();
		CredentialEntry c = p.getCredentialEntry("credential");
		try {
			username = new String(c.getUser().getValue(), "UTF-8");
			password = new String(c.getPassword().getValue(), "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new AdapterException(e1);
		}

		PropertyGroup connectionGroup = connectionInfo.getConnectionProperties();

		PropertyEntry expertMode = connectionGroup.getPropertyEntry(AdapterConstants.KEY_THIRDPARTY);

		listSystemData = (connectionGroup.getPropertyEntry(AdapterConstants.KEY_WITHSYS) != null)
				? AdapterConstants.BOOLEAN_TRUE.equalsIgnoreCase(
						connectionGroup.getPropertyEntry(AdapterConstants.KEY_WITHSYS).getValue())
				: false;
		nullableAsEmpty = (connectionGroup.getPropertyEntry(AdapterConstants.KEY_NULLASEMPTYSTRING) != null)
				? AdapterConstants.BOOLEAN_TRUE.equalsIgnoreCase(
						connectionGroup.getPropertyEntry(AdapterConstants.KEY_NULLASEMPTYSTRING).getValue())
				: false;

		String jdbcUrl = null;
		String jdbcClass = null;
		String jdbcJarFile = null;

		if (expertMode != null && AdapterConstants.BOOLEAN_TRUE.equalsIgnoreCase(expertMode.getValue())) {
			PropertyGroup expertGroup = connectionGroup.getPropertyGroup(AdapterConstants.KEY_GROUP_CUSTOM);
			jdbcUrl = expertGroup.getPropertyEntry(AdapterConstants.KEY_URL_CUSTOM).getValue();
			jdbcClass = expertGroup.getPropertyEntry(AdapterConstants.KEY_DRIVERCLASS_CUSTOM).getValue();
			jdbcJarFile = expertGroup.getPropertyEntry(AdapterConstants.KEY_JAR_CUSTOM).getValue();
		} else {
			jdbcClass = connectionGroup.getPropertyEntry(AdapterConstants.KEY_DRIVERCLASS).getValue();
			jdbcUrl = getJdbcUrl(connectionGroup);
		}

		blobHandle = new HashMap<Long, InputStream>();
		clobHandle = new HashMap<Long, Reader>();

		try {
			Driver d = null;

			if (expertMode != null && AdapterConstants.BOOLEAN_TRUE.equalsIgnoreCase(expertMode.getValue())) {
				File file = new File(jdbcJarFile);
				if (!file.exists())
					throw new AdapterException("File not found on the Agent Host at " + jdbcJarFile);

				URL u = new URL("jar:file:" + jdbcJarFile + "!/");
				URLClassLoader ucl = new URLClassLoader(new URL[] { u });
				d = (Driver) Class.forName(jdbcClass, true, ucl).newInstance();
			} else {
				d = (Driver) Class.forName(jdbcClass, true, this.getClass().getClassLoader()).newInstance();
			}

			DriverManager.registerDriver(new DriverDelegator(d));
			connection = DriverManager.getConnection(jdbcUrl, username, password);

			try {
				DatabaseMetaData dbms = connection.getMetaData();
				StringBuffer buffer = new StringBuffer();
				buffer.append(
						"Connected using driver class " + jdbcClass + " version [" + dbms.getDriverVersion() + "]\n");

				buffer.append("Database vendor [" + dbms.getDatabaseProductName() + "] Version ["
						+ dbms.getDatabaseProductVersion() + "]");

				logger.info(buffer.toString());
			} catch (SQLException e) {
				logger.warn("Unable to retrieve database information", e);
			}
			/*
			 * Forward-only allows the JDBC client to work more efficiently. But
			 * that is the default anyhow, hence no need to use stmt =
			 * conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.
			 * CONCUR_READ_ONLY)
			 * 
			 */
			stmt = connection.createStatement();
			stmt.setFetchSize(fetchSize);
		} catch (Exception e) {
			throw new AdapterException(e);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#prepareCall(com.sap.hana.dp.adapter.
	 * sdk.ProcedureMetadata)
	 */
	@Override
	public CallableProcedure prepareCall(ProcedureMetadata arg0) throws AdapterException {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#putNext(com.sap.hana.dp.adapter.sdk.
	 * AdapterRowSet)
	 */
	@Override
	public int putNext(AdapterRowSet arg0) throws AdapterException {
		logger.debug("In the function");
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#queryParameters(java.lang.String,
	 * java.util.List)
	 */
	@Override
	public ParametersResponse queryParameters(String arg0, List<Parameter> arg1) throws AdapterException {
		logger.debug("In the function");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#rollbackTransaction()
	 */
	@Override
	public void rollbackTransaction() throws AdapterException {
		logger.debug("In the function");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#setAutoCommit(boolean)
	 */
	@Override
	public void setAutoCommit(boolean arg0) throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#setBrowseNodeId(java.lang.String)
	 */
	@Override
	public void setBrowseNodeId(String arg0) throws AdapterException {
		this.nodeID = arg0;
		browseOffset = 0;

		closeLocalBrowseResultSet();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sap.hana.dp.adapter.sdk.Adapter#setFetchSize(int)
	 */
	@Override
	public void setFetchSize(int arg0) {
		this.fetchSize = arg0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#setNodesListFilter(com.sap.hana.dp.
	 * adapter.sdk.RemoteObjectsFilter)
	 */
	@Override
	public void setNodesListFilter(RemoteObjectsFilter arg0) throws AdapterException {
		logger.debug("In the function");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sap.hana.dp.adapter.sdk.Adapter#validateCall(com.sap.hana.dp.adapter.
	 * sdk.FunctionMetadata)
	 */
	@Override
	public void validateCall(FunctionMetadata arg0) throws AdapterException {
		logger.debug("In the function");

	}

	class DriverDelegator implements Driver {
		private Driver driver;

		DriverDelegator(Driver d) {
			this.driver = d;
		}

		public boolean acceptsURL(String u) throws SQLException {
			return this.driver.acceptsURL(u);
		}

		@Override
		public Connection connect(String u, Properties p) throws SQLException {
			return this.driver.connect(u, p);
		}

		@Override
		public int getMajorVersion() {
			return this.driver.getMajorVersion();
		}

		@Override
		public int getMinorVersion() {
			return this.driver.getMinorVersion();
		}

		@Override
		public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1) throws SQLException {
			return this.driver.getPropertyInfo(arg0, arg1);
		}

		@Override
		public boolean jdbcCompliant() {
			return this.driver.jdbcCompliant();
		}

		/**
		 * Allow on 1.7 java
		 * 
		 * @return
		 * @throws SQLFeatureNotSupportedException
		 */
		public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
			throw new SQLFeatureNotSupportedException();
		}
	}

}
