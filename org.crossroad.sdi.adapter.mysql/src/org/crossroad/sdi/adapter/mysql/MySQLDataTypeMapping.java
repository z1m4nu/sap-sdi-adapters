package org.crossroad.sdi.adapter.mysql;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.impl.AdapterConstants;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Column;

public class MySQLDataTypeMapping {

	static final Logger logger = LogManager.getLogger(MySQLDataTypeMapping.class);

	private Properties mappingTable = new Properties();

	public void init(String dataMapping) throws AdapterException {
		try {
			mappingTable.load(
					MySQLDataTypeMapping.class.getResourceAsStream("mapping.properties"));
			
			if (dataMapping != null) {
				mappingTable.load(new FileInputStream(dataMapping));
			}
			
			
			if (mappingTable.isEmpty())
			{
				throw new AdapterException("Data mapping not loaded or empty");
			}
			
			
		} catch (Exception e) {
			throw new AdapterException(e);
		}
		
	}

	public Column createColumn(String name, int jdbcType, String jdbcTypeName, int length, int precision, int scale) {
		System.out.println("Name [" + name + "] JDBC Type [" + jdbcType + "] JDBC Name [" + jdbcTypeName + "] Size ["
				+ length + "] Precision [" + precision + "] Scale [" + scale + "]");

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
		
		column.setDataType(DataType.valueOf(mappingTable.getProperty(jdbcTypeName, "INVALID")));

		// Custom definition
		if ("BIT".equalsIgnoreCase(jdbcTypeName)) {
			if (length == 0) {
				if (precision == 0) {
					column.setLength(64);
				} else {
					column.setLength(precision);
				}
			}
		}

//		switch (jdbcTypeName) {
//		case "BIT":
//			column.setDataType(DataType.VARCHAR);
//			if (length == 0) {
//				if (precision == 0) {
//					column.setLength(64);
//				} else {
//					column.setLength(precision);
//				}
//			}
//			break;
//		case "INT":
//			column.setDataType(DataType.INTEGER);
//			break;
//		case "INT UNSIGNED":
//		case "BIGINT":
//		case "BIGINT UNSIGNED":
//			column.setDataType(DataType.BIGINT);
//			break;
//		case "DOUBLE":
//			column.setDataType(DataType.DOUBLE);
//			break;
//		case "DECIMAL":
//			column.setDataType(DataType.DECIMAL);
//			break;
//		case "DECIMAL UNSIGNED":
//			column.setDataType(DataType.DOUBLE);
//			break;
//		case "FLOAT":
//			column.setDataType(DataType.REAL);
//			break;
//		case "TINYINT":
//		case "BOOL":
//		case "BOOLEAN":
//			column.setDataType(DataType.TINYINT);
//			break;
//		case "SMALLINT":
//		case "SMALLINT UNSIGNED":
//		case "MEDIUMINT":
//			column.setDataType(DataType.INTEGER);
//			break;
//		case "VARCHAR":
//		case "NVARCHAR":
//		case "CHAR":
//		case "TYNITEXT":
//			column.setDataType(DataType.NVARCHAR);
//			break;
//		case "LONGTEXT":
//		case "TEXT":
//		case "MEDIUMTEXT":
//			column.setDataType(DataType.NCLOB);
//			break;
//		case "DATETIME":
//		case "TIMESTAMP":
//			column.setDataType(DataType.TIMESTAMP);
//			break;
//		case "DATE":
//			column.setDataType(DataType.DATE);
//			break;
//		case "TIME":
//			column.setDataType(DataType.TIME);
//			break;
//		case "YEAR":
//			column.setDataType(DataType.INTEGER);
//			break;
//		// MySQL binary datatypes
//		case "BINARY":
//		case "VARBINARY":
//		case "TINBLOB":
//			column.setDataType(DataType.VARBINARY);
//			break;
//		case "LONGBLOB":
//		case "MEDIUMBLOB":
//		case "BLOB":
//			column.setDataType(DataType.BLOB);
//			break;
//		default:
//			throw new UnsupportedOperationException("Unsupported type <" + jdbcType + "> <" + jdbcTypeName + ">.");
//		}

		return column;
	}

}
