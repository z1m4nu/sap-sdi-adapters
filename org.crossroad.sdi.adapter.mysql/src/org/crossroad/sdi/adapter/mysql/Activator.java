/**
 * 
 */
package org.crossroad.sdi.adapter.mysql;

import java.util.ArrayList;
import java.util.List;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import com.sap.hana.dp.adapter.sdk.AdapterFactory;

/**
 * @author e.soden
 *
 */
public class Activator implements BundleActivator {
	private static BundleContext context;

	static BundleContext getContext() {
		return context;
	}

	List<ServiceRegistration<?>> services = new ArrayList<ServiceRegistration<?>>();
	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
	 */
	public void start(BundleContext bundleContext) throws Exception {
		Activator.context = bundleContext;
		services.add(context.registerService(AdapterFactory.class.getName(),new MySQLAdapterFactory() ,null));
	}

	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	public void stop(BundleContext bundleContext) throws Exception {
		Activator.context = null;
		for(ServiceRegistration<?> service:services)
		{
			service.unregister();
		}
	}
}
