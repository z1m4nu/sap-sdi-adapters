package org.crossroad.sdi.adapter.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Column;

public class MssqlToHanaTypeMap {
	static final Logger logger = LogManager.getLogger(MssqlToHanaTypeMap.class);
	static final String[] SYSTEM_TYPES = { "date", "time", "datetime2", "datetimeoffset", "tinyint", "smallint", "int",
			"smalldatetime", "real", "money", "datetime", "float", "sql_variant", "ntext", "bit", "decimal", "numeric",
			"smallmoney", "bigint", "varbinary", "varchar", "binary", "char", "timestamp", "nvarchar", "nchar", "xml",
			"image", "text", "hierarchyid", "geometry", "uniqueidentifier", "geography", "sysname" };

	public static int getMssqlTypeId(int jdbcTypeId, String typeName) {
		switch (jdbcTypeId) {
		case -4:
		case 2004:
			return 34;
		case -6:
			return 48;
		case 5:
			return 52;
		case 4:
			return 56;
		case 93:
			if (typeName.equalsIgnoreCase("smalldatetime")) {
				return 58;
			}
			if (typeName.equalsIgnoreCase("datetime")) {
				return 61;
			}
			if (typeName.equalsIgnoreCase("datetime2")) {
				return 42;
			}
			throw new UnsupportedOperationException(
					"Unsupported Mssql type <" + typeName + ">, JDBC type ID <" + jdbcTypeId + ">.");
		case 7:
			return 59;
		case 3:
			if (typeName.equalsIgnoreCase("decimal")) {
				return 106;
			}
			if (typeName.equalsIgnoreCase("money")) {
				return 60;
			}
			if (typeName.equalsIgnoreCase("smallmoney")) {
				return 122;
			}
			throw new UnsupportedOperationException(
					"Unsupported Mssql type <" + typeName + ">, JDBC type ID <" + jdbcTypeId + ">.");
		case 91:
			return 40;
		case 92:
			return 41;
		case 6:
		case 8:
			return 62;
		case 2000:
			return 98;
		case -16:
		case 2011:
			return 99;
		case -7:
			return 104;
		case 2:
			return 108;
		case -5:
			return 127;
		case -3:
			return 165;
		case 12:
			if (typeName.equalsIgnoreCase("varchar")) {
				return 167;
			}
			if (typeName.equalsIgnoreCase("nvarchar")) {
				return 231;
			}
			if (typeName.equalsIgnoreCase("uniqueidentifier")) {
				return 36;
			}
			if (typeName.equalsIgnoreCase("datetimeoffset")) {
				return 43;
			}
			throw new UnsupportedOperationException(
					"Unsupported Mssql type <" + typeName + ">, JDBC type ID <" + jdbcTypeId + ">.");
		case -2:
			return 173;
		case 1:
			return 175;
		case 1111:
			return 189;
		case -9:
			return 231;
		case -15:
			return 239;
		case -1:
		case 2005:
			if (typeName.equalsIgnoreCase("xml")) {
				return 241;
			}
			if (typeName.equalsIgnoreCase("text")) {
				return 35;
			}
			throw new UnsupportedOperationException(
					"Unsupported Mssql type <" + typeName + ">, JDBC type ID <" + jdbcTypeId + ">.");
		case -155:
			return 43;
		}
		throw new UnsupportedOperationException(
				"Unsupported Mssql type <" + typeName + ">, JDBC type ID <" + jdbcTypeId + ">.");
	}

	public static DataType getHanaDatatype(int mssqlTypeId, int size) {
		return getHanaDatatype(mssqlTypeId, size, false);
	}

	public static DataType getHanaDatatype(int mssqlTypeId, int size, boolean mapCharTypesToUnicode) {
		switch (mssqlTypeId) {
		case 48:
		case 104:
			return DataType.TINYINT;
		case 52:
			return DataType.SMALLINT;
		case 56:
			return DataType.INTEGER;
		case 62:
			return size < 25 ? DataType.REAL : DataType.DOUBLE;
		case 59:
			return DataType.REAL;
		case 60:
		case 106:
		case 108:
		case 122:
		case 127:
			return DataType.DECIMAL;
		case 36:
		case 43:
			return DataType.VARCHAR;
		case 58:
			return DataType.SECONDDATE;
		case 41:
		case 42:
		case 61:
			return DataType.TIMESTAMP;
		case 40:
			return DataType.DATE;
		case 189:
			return DataType.VARBINARY;
		case 34:
			return DataType.BLOB;
		case 35:
			return mapCharTypesToUnicode ? DataType.NCLOB : DataType.CLOB;
		case 99:
			return DataType.NCLOB;
		case 173:
			return size > 5000 ? DataType.BLOB : DataType.VARBINARY;
		case 165:
			return size > 5000 ? DataType.BLOB : DataType.VARBINARY;
		case 175:
			if (mapCharTypesToUnicode) {
				return size > 5000 ? DataType.NCLOB : DataType.NVARCHAR;
			}
			return size > 5000 ? DataType.CLOB : DataType.VARCHAR;
		case 167:
			if (mapCharTypesToUnicode) {
				return size > 5000 ? DataType.NCLOB : DataType.NVARCHAR;
			}
			return size > 5000 ? DataType.CLOB : DataType.VARCHAR;
		case 239:
			return DataType.NVARCHAR;
		case 231:
			return size == Integer.MAX_VALUE ? DataType.NCLOB : DataType.NVARCHAR;
		}
		throw new UnsupportedOperationException("Unsupported Mssql type <" + mssqlTypeId + ">.");
	}

	public static void setDatatypeParameters(Column column, int mssqlTypeId, int size, int precision, int scale) {
		DataType hanaType = column.getDataType();
		switch (hanaType) {
		case VARCHAR:
			switch (mssqlTypeId) {
			case 127:
				column.setPrecision(19);
				column.setNativeLength(size);
				column.setLength(size);
				break;
			case 60:
				column.setPrecision(19);
				column.setScale(4);
				column.setNativeLength(size);
				column.setLength(size);
				break;
			case 122:
				column.setPrecision(10);
				column.setScale(4);
				column.setNativeLength(size);
				column.setLength(size);
				break;
			case 106:
			case 108:
				column.setPrecision(precision);
				column.setScale(scale);
				column.setLength(size);
			}
			break;
		case CLOB:
			switch (mssqlTypeId) {
			case 36:
				column.setLength(36);
				column.setNativeLength(size);
				break;
			case 43:
				column.setLength(34);
				column.setNativeLength(size);
				break;
			default:
				column.setLength(size);
				column.setNativeLength(size);
			}
			break;
		case DOUBLE:
			column.setLength(size);
			column.setNativeLength(size);
			break;
		case DATE:
			if (mssqlTypeId == 189) {
				column.setLength(8);
				column.setNativeLength(size);
			} else {
				column.setLength(size);
				column.setNativeLength(size);
			}
			break;
		case DECIMAL:
			column.setPrecision(size);
			column.setScale(scale);
			column.setNativeLength(size);
			column.setLength(size);
			break;
		case TIMESTAMP:
			if (mssqlTypeId == 61)
			{
				column.setPrecision(7);
				column.setNativeLength(size);
				column.setLength(size);
			}
			break;
		}
	}

	public static String findSystemDataTypeName(String originalType, Connection connection) throws AdapterException {
		String sysTypeName;
		ResultSet typeNameRs = null;
		PreparedStatement stmt = null;
		sysTypeName = null;
		typeNameRs = null;
		stmt = null;

		if (isSystemType(originalType)) {
			return sysTypeName;
		}

		try {
			stmt = connection.prepareStatement(
					"select stype.name s_name, utype.*  from sys.types stype join sys.types utype on utype .system_type_id = stype.system_type_id where utype.is_user_defined = 1 and stype.is_user_defined = 0 and utype.name = ?");
			stmt.setString(1, originalType);
			typeNameRs = stmt.executeQuery();
			if (typeNameRs.next())
				sysTypeName = typeNameRs.getString("s_name");
		} catch (SQLException _ex) {
			logger.warn((new StringBuilder("Unable to find MSSQL type <")).append(originalType).append(">").toString());
			throw new AdapterException(
					(new StringBuilder("Unable to find Mssql type <")).append(originalType).append(">").toString());
		} finally {
			try {
				if (typeNameRs != null)
					typeNameRs.close();
			} catch (SQLException e) {
				logger.warn("Fails to close ResultSet.", e);
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				logger.warn("Fails to close PrepareStatement.", e);
			}
		}
		return sysTypeName;
	}

	private static boolean isSystemType(String typeName) {
		String[] as;
		int j = (as = SYSTEM_TYPES).length;
		for (int i = 0; i < j; i++) {
			String sysType = as[i];
			if (sysType.equalsIgnoreCase(typeName)) {
				return true;
			}
		}
		return false;
	}

}
