package org.crossroad.sdi.adapter.mysql;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.Column;

public class MySQLHANAType {

	static final Logger logger = LogManager.getLogger(MySQLHANAType.class);

	public static Column buildMySQLColumn(String name, int jdbcType, String jdbcTypeName, int length, int precision,
			int scale) {
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

		switch (jdbcTypeName) {
		case "BIT":
			column.setDataType(DataType.VARCHAR);
			if (length == 0) {
				if (precision == 0) {
					column.setLength(64);
				} else {
					column.setLength(precision);
				}
			}
			break;
		case "INT":
			column.setDataType(DataType.INTEGER);
			break;
		case "INT UNSIGNED":
		case "BIGINT":
		case "BIGINT UNSIGNED":
			column.setDataType(DataType.BIGINT);
			break;
		case "DOUBLE":
			column.setDataType(DataType.DOUBLE);
			break;
		case "DECIMAL":
			column.setDataType(DataType.DECIMAL);
			break;
		case "DECIMAL UNSIGNED":
			column.setDataType(DataType.DOUBLE);
			break;
		case "FLOAT":
			column.setDataType(DataType.REAL);
			break;
		case "TINYINT":
		case "BOOL":
		case "BOOLEAN":
			column.setDataType(DataType.TINYINT);
			break;
		case "SMALLINT":
		case "SMALLINT UNSIGNED":
		case "MEDIUMINT":
			column.setDataType(DataType.INTEGER);
			break;
		case "VARCHAR":
		case "NVARCHAR":
		case "CHAR":
		case "TYNITEXT":
			column.setDataType(DataType.NVARCHAR);
			break;
		case "LONGTEXT":
		case "TEXT":
		case "MEDIUMTEXT":
			column.setDataType(DataType.NCLOB);
			break;
		case "DATETIME":
		case "TIMESTAMP":
			column.setDataType(DataType.TIMESTAMP);
			break;
		case "DATE":
			column.setDataType(DataType.DATE);
			break;
		case "TIME":
			column.setDataType(DataType.TIME);
			break;
		case "YEAR":
			column.setDataType(DataType.INTEGER);
			break;
		// MySQL binary datatypes
		case "BINARY":
		case "VARBINARY":
		case "TINBLOB":
			column.setDataType(DataType.VARBINARY);
			break;
		case "LONGBLOB":
		case "MEDIUMBLOB":
		case "BLOB":
			column.setDataType(DataType.BLOB);
			break;
		default:
			throw new UnsupportedOperationException("Unsupported type <" + jdbcType + "> <" + jdbcTypeName + ">.");
		}

		

		return column;
	}

}
