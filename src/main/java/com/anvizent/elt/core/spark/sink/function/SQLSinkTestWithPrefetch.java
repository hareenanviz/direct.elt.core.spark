package com.anvizent.elt.core.spark.sink.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.constant.MySQLStoreDataType;
import com.anvizent.elt.core.lib.constant.StoreType;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.DBCheckMode;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.constant.DBWriteMode;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkTestWithPrefetch {

	private static RDBMSConnection rdbmsConnection = new RDBMSConnection("jdbc:mysql://localhost:8889/MigSchedul_1010611?useSSL=false", "root", "root",
	        "com.mysql.jdbc.Driver");
//	private static RDBMSConnection rdbmsConnection = new RDBMSConnection("jdbc:mysql://192.168.0.124:4475/MigSchedul_1010611?useSSL=false", "almadmin",
//	        "Ewc@4fvQ#pT5", "com.mysql.jdbc.Driver");

	public static void main(String[] args) throws Exception {
		Class.forName(rdbmsConnection.getDriver());

		System.out.println("Connecting to a selected database...");
		Connection conn = DriverManager.getConnection(rdbmsConnection.getJdbcURL(), rdbmsConnection.getUserName(), rdbmsConnection.getPassword());
		System.out.println("Connected database successfully...");

		System.out.println("Creating select statement...");
		Statement stmt = conn.createStatement();

		String sql = "SELECT * FROM Products";
		System.out.println("Executing select statement...");
		ResultSet rs = stmt.executeQuery(sql);
		System.out.println("Getting metadata...");
		ResultSetMetaData rsm = rs.getMetaData();
		int columnCount = rsm.getColumnCount();

		LinkedHashMap<String, AnvizentDataType> structure = new LinkedHashMap<>();
		for (int i = 1; i <= columnCount; i++) {
			structure.put(rsm.getColumnLabel(i), new AnvizentDataType(StoreType.MYSQL, MySQLStoreDataType.valueOf(rsm.getColumnTypeName(i)).name(), ""));
		}
		List<HashMap<String, Object>> rows = new LinkedList<HashMap<String, Object>>();

		while (rs.next()) {
			HashMap<String, Object> row = new HashMap<>();

			ArrayList<String> dates = new ArrayList<String>();
			dates.add("CreatedDateUtc");
			dates.add("ModifiedDateUtc");
			dates.add("Added_Date");
			dates.add("Updated_Date");

			for (int i = 1; i <= columnCount; i++) {
				String column = rsm.getColumnLabel(i);
				if (!dates.contains(column)) {
					Object value = rs.getObject(column);
					row.put(column, value);
				}
			}

			rows.add(row);
		}

		System.out.println("\n\n\n\n" + rows.size());

		SQLSinkConfigBean sqlSinkConfigBean = new SQLSinkConfigBean();
		sqlSinkConfigBean.setAlwaysUpdate(false);
		sqlSinkConfigBean.setBatchSize(5000l);
		sqlSinkConfigBean.setBatchType(BatchType.BATCH_BY_SIZE);
		sqlSinkConfigBean.setDBCheckMode(DBCheckMode.PREFECTH);
		sqlSinkConfigBean.setDBInsertMode(DBInsertMode.UPSERT);
		sqlSinkConfigBean.setDBWriteMode(DBWriteMode.APPEND);
		sqlSinkConfigBean.setDestroyRetryConfigBean(3, 1000l);
		sqlSinkConfigBean.setInitRetryConfigBean(3, 1000l);
		sqlSinkConfigBean.setInsertQuery("");
		ArrayList<String> keyFields = new ArrayList<String>();
		keyFields.add("Attributes_Name");
		keyFields.add("Company_Id");
		keyFields.add("Id");
		keyFields.add("DataSource_Id");
		keyFields.add("Products_Key");

		sqlSinkConfigBean.setKeyColumns(keyFields);
		sqlSinkConfigBean.setKeyFields(keyFields);
		sqlSinkConfigBean.setMaxElementsInMemory(20000);
		sqlSinkConfigBean.setMaxRetryCount(3);
		sqlSinkConfigBean.setPrefetchBatchSize(-1);
		sqlSinkConfigBean.setRdbmsConnection(rdbmsConnection);
		sqlSinkConfigBean.setSelectColumns(getSelectColumns());
		sqlSinkConfigBean.setSelectFields(getSelectColumns());
		sqlSinkConfigBean.setRowFields(getSelectColumns());
		sqlSinkConfigBean.setSelectAllQuery("SELECT * FROM Products_test");
		sqlSinkConfigBean.setTableName("Products_test");
		sqlSinkConfigBean.setUpsertQueryInfo(getUpsertQueryInfo());
		sqlSinkConfigBean.setUpdateQuery(sqlSinkConfigBean.getUpsertQueryInfo().getQuery());

		SQLSinkPrefetchUpsertBatchFunction batchFunction = new SQLSinkPrefetchUpsertBatchFunction(sqlSinkConfigBean, structure, null, null, null);
		batchFunction.call(rows.iterator());
	}

	private static ArrayList<String> getSelectColumns() {
		ArrayList<String> columns = new ArrayList<>();

		columns.add("Attributes_Value");
		columns.add("Brand");
		columns.add("Classification");
		columns.add("Code");
		columns.add("Cost");
//		columns.add("CreatedDateUtc");
		columns.add("Description");
		columns.add("DisableQuantitySync");
		columns.add("IncrementalQuantity");
		columns.add("IsAlternateCode");
		columns.add("IsAlternateSKU");
		columns.add("LongDescription");
//		columns.add("ModifiedDateUtc");
		columns.add("MOQ");
		columns.add("MOQInfo");
		columns.add("PartNumber");
		columns.add("QuantityAvailable");
		columns.add("QuantityInbound");
		columns.add("QuantityIncoming");
		columns.add("QuantityInStock");
		columns.add("QuantityOnHand");
		columns.add("QuantityOnHold");
		columns.add("QuantityPending");
		columns.add("QuantityPicked");
		columns.add("QuantityTotalFBA");
		columns.add("QuantityTransfer");
		columns.add("ReorderPoint");
		columns.add("RetailPrice");
		columns.add("SalePrice");
		columns.add("ShortDescription");
		columns.add("Sku");
		columns.add("Supplier");
		columns.add("SupplierInfo_Cost");
		columns.add("SupplierInfo_IsActive");
		columns.add("SupplierInfo_IsPrimary");
		columns.add("SupplierInfo_LeadTime");
		columns.add("SupplierInfo_SupplierName");
		columns.add("SupplierInfo_SupplierPartNumber");
		columns.add("VariationParentSku");
		columns.add("WeightUnit");
		columns.add("WeightValue");
		columns.add("Source_Hash_Value");
		columns.add("Mismatch_Flag");
//		columns.add("Added_Date");
		columns.add("Added_User");
//		columns.add("Updated_Date");
		columns.add("Updated_User");

		return columns;
	}

	private static QueryInfo getUpsertQueryInfo() {
		QueryInfo queryInfo = new QueryInfo();
		HashMap<String, List<Integer>> map = new HashMap<>();

		map.put("DataSource_Id", Arrays.asList(1, 52));
		map.put("Products_Key", Arrays.asList(2, 53));
		map.put("Attributes_Name", Arrays.asList(3, 49));
		map.put("Attributes_Value", Arrays.asList(4));
		map.put("Brand", Arrays.asList(5));
		map.put("Classification", Arrays.asList(6));
		map.put("Code", Arrays.asList(7));
		map.put("Company_Id", Arrays.asList(8, 50));
		map.put("Cost", Arrays.asList(9));
//		map.put("CreatedDateUtc", Arrays.asList(10));
		map.put("Description", Arrays.asList(10));
		map.put("DisableQuantitySync", Arrays.asList(11));
		map.put("Id", Arrays.asList(12, 51));
		map.put("IncrementalQuantity", Arrays.asList(13));
		map.put("IsAlternateCode", Arrays.asList(14));
		map.put("IsAlternateSKU", Arrays.asList(15));
		map.put("LongDescription", Arrays.asList(16));
//		map.put("ModifiedDateUtc", Arrays.asList(17));
		map.put("MOQ", Arrays.asList(17));
		map.put("MOQInfo", Arrays.asList(18));
		map.put("PartNumber", Arrays.asList(19));
		map.put("QuantityAvailable", Arrays.asList(20));
		map.put("QuantityInbound", Arrays.asList(21));
		map.put("QuantityIncoming", Arrays.asList(22));
		map.put("QuantityInStock", Arrays.asList(23));
		map.put("QuantityOnHand", Arrays.asList(24));
		map.put("QuantityOnHold", Arrays.asList(25));
		map.put("QuantityPending", Arrays.asList(26));
		map.put("QuantityPicked", Arrays.asList(27));
		map.put("QuantityTotalFBA", Arrays.asList(28));
		map.put("QuantityTransfer", Arrays.asList(29));
		map.put("ReorderPoint", Arrays.asList(30));
		map.put("RetailPrice", Arrays.asList(31));
		map.put("SalePrice", Arrays.asList(32));
		map.put("ShortDescription", Arrays.asList(33));
		map.put("Sku", Arrays.asList(34));
		map.put("Supplier", Arrays.asList(35));
		map.put("SupplierInfo_Cost", Arrays.asList(36));
		map.put("SupplierInfo_IsActive", Arrays.asList(37));
		map.put("SupplierInfo_IsPrimary", Arrays.asList(38));
		map.put("SupplierInfo_LeadTime", Arrays.asList(39));
		map.put("SupplierInfo_SupplierName", Arrays.asList(40));
		map.put("SupplierInfo_SupplierPartNumber", Arrays.asList(41));
		map.put("VariationParentSku", Arrays.asList(42));
		map.put("WeightUnit", Arrays.asList(43));
		map.put("WeightValue", Arrays.asList(44));
		map.put("Source_Hash_Value", Arrays.asList(45));
		map.put("Mismatch_Flag", Arrays.asList(46));
//		map.put("Added_Date", Arrays.asList(47));
		map.put("Added_User", Arrays.asList(47));
//		map.put("Updated_Date", Arrays.asList(48));
		map.put("Updated_User", Arrays.asList(48));

		queryInfo.setMultiValuedFieldIndexesToSet(map);
		queryInfo.setQuery("UPDATE `Products_test` SET `Attributes_Value`=?, `Brand`=?, " + "`Classification`=?, `Code`=?, `Cost`=?, "
//		        + "`CreatedDateUtc`=?, "
		        + "`Description`=?, `DisableQuantitySync`=?, `IncrementalQuantity`=?, " + "`IsAlternateCode`=?, `IsAlternateSKU`=?, `LongDescription`=?, "
//		        + "`ModifiedDateUtc`=?, "
		        + "`MOQ`=?, `MOQInfo`=?, `PartNumber`=?, `QuantityAvailable`=?, "
		        + "`QuantityInbound`=?, `QuantityIncoming`=?, `QuantityInStock`=?, `QuantityOnHand`=?, `QuantityOnHold`=?, `QuantityPending`=?, `QuantityPicked`=?, "
		        + "`QuantityTotalFBA`=?, `QuantityTransfer`=?, `ReorderPoint`=?, `RetailPrice`=?, `SalePrice`=?, `ShortDescription`=?, `Sku`=?, `Supplier`=?, "
		        + "`SupplierInfo_Cost`=?, `SupplierInfo_IsActive`=?, `SupplierInfo_IsPrimary`=?, `SupplierInfo_LeadTime`=?, `SupplierInfo_SupplierName`=?, "
		        + "`SupplierInfo_SupplierPartNumber`=?, `VariationParentSku`=?, `WeightUnit`=?, `WeightValue`=?, `Source_Hash_Value`=?, `Mismatch_Flag`=?, "
//		        + "`Added_Date`=?, "
		        + "`Added_User`=?, `Updated_Date`=?, `Updated_User`=? WHERE `Attributes_Name`=? AND `Company_Id`=? AND `Id`=? AND `DataSource_Id`=? "
		        + "AND `Products_Key`=?");

		return queryInfo;
	}

}
