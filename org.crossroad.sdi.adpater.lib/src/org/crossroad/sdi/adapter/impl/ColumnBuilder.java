/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Column;

/**
 * @author e.soden
 *
 */
public class ColumnBuilder {
	static final Logger logger = LogManager.getLogger("ColumnBuilder");

	private Properties mappingTable = new Properties();

	/**
	 * 
	 */
	public ColumnBuilder() {

	}

	public void loadMapping(InputStream dataMapping) throws AdapterException {
		try {
			mappingTable.load(ColumnBuilder.class.getResourceAsStream("mapping.properties"));

			if (dataMapping != null) {
				mappingTable.load(dataMapping);
			}

			if (mappingTable.isEmpty()) {
				throw new AdapterException("Data mapping not loaded or empty");
			}

		} catch (Exception e) {
			throw new AdapterException(e);
		}

	}

	public Column createColumn(String name, int jdbcType, String jdbcTypeName, int length, int precision, int scale) {

		Column column = new Column();

		column.setName(name);

		if (precision > 0) {
			column.setPrecision(precision);
		}

		if (scale > 0) {
			column.setScale(scale);
		}

		column.setLength(length);
		column.setNativeLength(length);

		jdbcTypeName = jdbcTypeName.replace(" ", "");

		column.setDataType(DataType.valueOf(mappingTable.getProperty(jdbcTypeName, DataType.INVALID.name())));

		logger.info("Name [" + name + "] JDBC Type [" + jdbcType + "] JDBC Name [" + jdbcTypeName + "] HANA Type ["
				+ column.getDataType().name() + "]");
		return column;
	}
}
