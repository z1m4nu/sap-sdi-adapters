package org.crossroad.sdi.adapter.jdbc;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapterFactory;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class JDBCAdapterFactory extends AbstractJDBCAdapterFactory {
	public JDBCAdapterFactory() {
		super();
	}
	
	@Override
	public Adapter createAdapterInstance() {
		return new JDBCAdapter();
	}

	@Override
	public String getAdapterType() {
		return "JDBC - Generic Adapter";
	}

	@Override
	public String getAdapterDisplayName() {
		return "Generic JDBDC Database Adapter";
	}

	@Override
	public String getAdapterDescription() {
		return "Basic JDBC connection no optimisation";
	}

	@Override
	public RemoteSourceDescription getAdapterConfig() {
		return null;
	}

	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription propertyGroup)
			throws AdapterException {
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

}
