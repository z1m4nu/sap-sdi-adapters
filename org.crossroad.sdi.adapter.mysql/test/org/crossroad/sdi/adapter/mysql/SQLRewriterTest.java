/**
 * 
 */
package org.crossroad.sdi.adapter.mysql;

/**
 * @author e.soden
 *
 */
public class SQLRewriterTest {

	/**
	 * 
	 */
	public SQLRewriterTest() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SQLRewriterTest test = new SQLRewriterTest();

		test.test();
	}
	
	public void test() throws Exception
	{
		String sql = "SELECT \"catalog_category_entity_varchar\".\"V2\" FROM (\"tally_db_new.<none>.catalog_category_entity\" \"catalog_category_entity\"  LEFT OUTER JOIN  (SELECT \"catalog_category_entity_varchar\".\"row_id\" AS \"V1\", \"catalog_category_entity_varchar\".\"value\" AS \"V2\" FROM \"tally_db_new.<none>.catalog_category_entity_varchar\" \"catalog_category_entity_varchar\" WHERE \"catalog_category_entity_varchar\".\"attribute_id\" = 45 AND \"catalog_category_entity_varchar\".\"store_id\" = 3 AND \"catalog_category_entity_varchar\".\"value\" != 'SAP Import' )  \"catalog_category_entity_varchar\"  ON (\"catalog_category_entity\".\"row_id\" = \"catalog_category_entity_varchar\".\"V1\") ) WHERE \"catalog_category_entity\".\"parent_id\" = 2 GROUP BY \"catalog_category_entity_varchar\".\"V2\" ORDER BY \"catalog_category_entity_varchar\".\"V2\" ASC LIMIT 200";
		System.out.println("From ["+sql+"]");
		SQLRewriter rewriter = new SQLRewriter(128);
		
		String out = rewriter.rewriteSQL(sql);
		System.out.println("To ["+out+"]");
	}

}
