/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

/**
 * @author e.soden
 *
 */
public final class AdapterConstants {
	public static final String KEY_THIRDPARTY = "jdbc.thirdparty.custom";
	public static final String KEY_JAR_CUSTOM = "jdbc.jarfile.custom";
	public static final String KEY_DRIVERCLASS_CUSTOM = "jdbc.driverclass.custom";
	public static final String KEY_URL_CUSTOM = "jdbc.url.custom";

	public static final String KEY_GROUP_MAIN = "jdbc.main";
	public static final String KEY_GROUP_CUSTOM = "jdbc.expert";
	public static final String KEY_GROUP_NORMAL = "jdbc.normal";
	public static final String KEY_GROUP_CONNECTION = "jdbc.connection";

	public static final String KEY_DRIVERCLASS = "jdbc.driverclass";

	public static final String KEY_HOSTNAME = "jdbc.host";
	public static final String KEY_PORT = "jdbc.port";
	public static final String KEY_DATABASE = "jdbc.dbname";
	public static final String KEY_OPTION = "jdbc.option";
	public static final String KEY_WITHSYS = "jdbc.systables";
	public static final String KEY_NULLASEMPTYSTRING = "jdbc.nullasemptystring";
	
	
	
	
	public static final String PRP_SCHEMA = "schema";
	public static final String PRP_CATALOG = "catalog";
	public static final String PRP_TABLE = "table";
	public static final String PRP_UNIQNAME = "uniquename";
	public static final String BOOLEAN_TRUE = "true";
	public static final String BOOLEAN_FALSE = "false";

	public static final String NULL_AS_STRING = "<none>";

	public static final int CATALOG_EXPANDED = 0;
	public static final int SCHEMA_EXPANDED = 1;
	public static final int TABLE_EXPANDED = 2;
	public static final int NULL_EXPANDED = -1;

	public static final String FORMAT_FULLDATE = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String FORMAT_DATEONLY = "yyyy-MM-dd";
	public static final String FORMAT_TIMEONLY = "HH:mm:ss.SSS";
	public static final String KEY_THREADPOOL_SIZE = "jdbc.threadpool.size";

	public static final String REMOTESOURCE_VERSION = "1.0.1";

	/**
	 * 
	 */
	private AdapterConstants() {
		// TODO Auto-generated constructor stub
	}

}
