/**
 * 
 */
package org.crossroad.sdi.adapter.mysql;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

/**
 * @author e.soden
 *
 */
public class MySQLAdapterFactory implements AdapterFactory {
	static Logger logger = LogManager.getLogger(MySQLAdapterFactory.class);
	private static final String NAME = "DBGMYSQL";//"JDBC - MySQL Server";

	@Override
	public Adapter createAdapterInstance() {
		return new MySQLAdapter();
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
		return "DP Adapter MySQLAdapter";
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
