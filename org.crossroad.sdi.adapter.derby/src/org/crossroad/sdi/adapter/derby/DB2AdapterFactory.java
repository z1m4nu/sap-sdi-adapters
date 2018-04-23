/**
 * 
 */
package org.crossroad.sdi.adapter.derby;

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
public class DB2AdapterFactory implements AdapterFactory {
	static Logger logger = LogManager.getLogger(DB2AdapterFactory.class);
	/**
	 * 
	 */
	public DB2AdapterFactory() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.sap.hana.dp.adapter.sdk.AdapterFactory#createAdapterInstance()
	 */
	@Override
	public Adapter createAdapterInstance() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.sap.hana.dp.adapter.sdk.AdapterFactory#getAdapterConfig()
	 */
	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.sap.hana.dp.adapter.sdk.AdapterFactory#getAdapterDescription()
	 */
	@Override
	public String getAdapterDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.sap.hana.dp.adapter.sdk.AdapterFactory#getAdapterDisplayName()
	 */
	@Override
	public String getAdapterDisplayName() {
		return "JDBC - DB2 Adapter";
	}

	/* (non-Javadoc)
	 * @see com.sap.hana.dp.adapter.sdk.AdapterFactory#getAdapterType()
	 */
	@Override
	public String getAdapterType() {
		return "JDBC - DB2 Adapter";
	}

	/* (non-Javadoc)
	 * @see com.sap.hana.dp.adapter.sdk.AdapterFactory#upgrade(com.sap.hana.dp.adapter.sdk.RemoteSourceDescription)
	 */
	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.sap.hana.dp.adapter.sdk.AdapterFactory#validateAdapterConfig(com.sap.hana.dp.adapter.sdk.RemoteSourceDescription)
	 */
	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription arg0) throws AdapterException {
		// TODO Auto-generated method stub
		return false;
	}

}
