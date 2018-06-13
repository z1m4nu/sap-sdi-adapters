/**
 * 
 */
package org.crossroad.sdi.adapter;

import org.crossroad.sdi.adapter.jdbc.JDBCStatementConverter;

/**
 * @author e.soden
 *
 */
public class SQLStatementTest {

	/**
	 * 
	 */
	public SQLStatementTest() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{

		JDBCStatementConverter convert = new JDBCStatementConverter(128);
		String str = convert.convertSQL("SELECT \"T0\".\"sku\", \"T0\".\"product_status\", \"T0\".\"warehouse\", \"T0\".\"location\", \"T0\".\"qty\" FROM \"tally_db_new.<none>.tally_bw_stock_report\" \"T0\"", null);
		
		System.out.println("stri ["+str+"]");
		
		str = convert.convertSQL("SELECT \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL1\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL2\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL3\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL4\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL5\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL6\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL7\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL8\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL9\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL10\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL11\", \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\".\"COL12\" FROM \"EIT_Promotion_Upload.xlsx/promo template\" \"ZVT_EIT_EXCEL_PROMOTION_UPLOAD\"", null);
		
		System.out.println("str ["+str+"]");
	}

}
