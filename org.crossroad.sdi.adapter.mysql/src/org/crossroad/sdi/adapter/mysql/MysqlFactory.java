/**
 * (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.crossroad.sdi.adapter.mysql;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapterFactory;
import org.crossroad.sdi.adapter.impl.RequiredComponents;
import org.osgi.framework.BundleContext;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class MysqlFactory extends AbstractJDBCAdapterFactory{
	private static final String NAME = "JDBC - MySQL Server";

	public MysqlFactory(BundleContext context) {
		super(context);
		
	}



	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAdapterDescription() {
		return "DP Adapter MySQLAdapter";
	}

	@Override
	public String getAdapterDisplayName() {
		return NAME;
	}

	@Override
	public String getAdapterType() {
		return NAME;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RequiredComponents getRequiredComponents() {
		RequiredComponents components = new RequiredComponents();
		components.addClass(new String[] {"com.mysql.jdbc.Driver","com.mysql.cj.jdbc.Driver"}, false);
		components.addPatternLibrary("mysql-connector.*\\.jar");
		return components;
	}



	@Override
	protected Adapter doCreateAdapterInstance() {
		return new MySQLAdapter();
	}




}
