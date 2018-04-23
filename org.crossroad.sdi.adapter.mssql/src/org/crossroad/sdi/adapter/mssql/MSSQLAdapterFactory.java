package org.crossroad.sdi.adapter.mssql;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class MSSQLAdapterFactory implements AdapterFactory {
	static Logger logger = LogManager.getLogger(MSSQLAdapterFactory.class);

	@Override
	public Adapter createAdapterInstance() {
		return new MSSQLAdapter();
	}

	@Override
	public String getAdapterType() {
		return "JDBC - Microsoft SQL Server";
	}

	@Override
	public String getAdapterDisplayName() {
		return "JDBC - Microsoft SQL Server";
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
}
