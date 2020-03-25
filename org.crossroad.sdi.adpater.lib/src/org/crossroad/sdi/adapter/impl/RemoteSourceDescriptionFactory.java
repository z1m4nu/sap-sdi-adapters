/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;

/**
 * @author e.soden
 *
 */
public final class RemoteSourceDescriptionFactory {

	/**
	 * 
	 */
	private RemoteSourceDescriptionFactory() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 * @return
	 */
	public static PropertyGroup getBasicJDBCConnectionGroup() {
		PropertyGroup connectGroup = new PropertyGroup(AdapterConstants.KEY_GROUP_CONNECTION, "JDBC connection");
		PropertyEntry hostEntry = new PropertyEntry(AdapterConstants.KEY_HOSTNAME, "Hostname", "Database Hostname", true);
		PropertyEntry portEntry = new PropertyEntry(AdapterConstants.KEY_PORT, "TCP Port", "Database listen port", true);
		PropertyEntry dbsEntry = new PropertyEntry(AdapterConstants.KEY_DATABASE, "Database name", "Database name", true);
		PropertyEntry optionEntry = new PropertyEntry(AdapterConstants.KEY_OPTION, "Connection option", "Set JDBC Url optional", false);
		PropertyEntry onlySysData = new PropertyEntry(AdapterConstants.KEY_WITHSYS, "Display system data",
				"Allow to show or not system data", false);
		onlySysData.addChoice(AdapterConstants.BOOLEAN_TRUE, AdapterConstants.BOOLEAN_TRUE);
		onlySysData.addChoice(AdapterConstants.BOOLEAN_FALSE, AdapterConstants.BOOLEAN_FALSE);
		onlySysData.setDefaultValue(AdapterConstants.BOOLEAN_FALSE);

		PropertyEntry nullableData = new PropertyEntry(AdapterConstants.KEY_NULLASEMPTYSTRING, "NULL as empty data",
				"Allow to show null data as empty", false);
		nullableData.addChoice(AdapterConstants.BOOLEAN_TRUE, AdapterConstants.BOOLEAN_TRUE);
		nullableData.addChoice(AdapterConstants.BOOLEAN_FALSE, AdapterConstants.BOOLEAN_FALSE);
		nullableData.setDefaultValue(AdapterConstants.BOOLEAN_FALSE);
		
		
		
		connectGroup.addProperty(hostEntry);
		connectGroup.addProperty(portEntry);
		connectGroup.addProperty(dbsEntry);
		connectGroup.addProperty(optionEntry);
		connectGroup.addProperty(onlySysData);
		connectGroup.addProperty(nullableData);

		return connectGroup;
	}
	
	
	
	public static PropertyGroup getExpertGroup()
	{
		PropertyGroup expertGroup = new PropertyGroup(AdapterConstants.KEY_GROUP_CUSTOM, "Expert mode");
		PropertyEntry jdbcurl = new PropertyEntry(AdapterConstants.KEY_URL_CUSTOM, "URL",
				"The URL of the connection, e.g. jdbc:sqlserver://localhost;databaseName=master");
		expertGroup.addProperty(jdbcurl);

		PropertyEntry jdbcDrv = new PropertyEntry(AdapterConstants.KEY_DRIVERCLASS_CUSTOM, "Driver Class",
				"The class name to use, e.g. com.microsoft.sqlserver.jdbc.SQLServerDriver");
		expertGroup.addProperty(jdbcDrv);

		PropertyEntry jdbcJar = new PropertyEntry(AdapterConstants.KEY_JAR_CUSTOM, "JDBC Driver jar file",
				"the location of the jdbc driver's jar file on the agent computer, e.g. lib/sqljdbc.jar");
		expertGroup.addProperty(jdbcJar);

		
		
		return expertGroup;
	}
	
	/**
	 * 
	 * @return
	 */
	public static CredentialProperties getCredentialProperties() {
		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "JDBC Credentials");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		
		credentialProperties.addCredentialEntry(credential);

		return credentialProperties;
	}
	
	
}
