package org.crossroad.sdi.adapter.impl;

import java.util.Map;
import java.util.Set;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;

public interface ISQLRewriter {

	void setColumnHelper(ColumnHelper helper);

	String rewriteSQL(String sql) throws AdapterException;
	
	public ExpressionBase.Type getQueryType();

}