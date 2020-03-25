package org.crossroad.sdi.adapter.mssql;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapterFactory;
import org.crossroad.sdi.adapter.impl.RequiredComponents;
import org.osgi.framework.BundleContext;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class MSSQLAdapterFactory extends AbstractJDBCAdapterFactory {

	private static final String NAME = "JDBC - Microsoft SQL Server";

	public MSSQLAdapterFactory(BundleContext context) {
		super(context);
	}


	@Override
	public String getAdapterType() {
		return NAME;
	}

	@Override
	public String getAdapterDisplayName() {
		return NAME;
	}

	@Override
	public String getAdapterDescription() {
		return "DP Adapter MSSQLAdapter";
	}

	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		return null;
	}

	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RequiredComponents getRequiredComponents() {
		RequiredComponents components = new RequiredComponents();
		components.addClass(new String[] {"com.microsoft.sqlserver.jdbc.SQLServerDriver"}, true);
		components.addPatternLibrary("sqljdbc.*\\.jar");
		components.addPatternLibrary("mssql-jdbc-.*\\.jar");
		return components;
	}

	@Override
	protected Adapter doCreateAdapterInstance() {
		return new MSSQLAdapter();
	}


}
