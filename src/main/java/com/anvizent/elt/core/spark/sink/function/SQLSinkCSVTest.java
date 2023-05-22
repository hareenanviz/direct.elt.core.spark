package com.anvizent.elt.core.spark.sink.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.univocity.parsers.csv.CsvWriterSettings;

public class SQLSinkCSVTest {

	public static void main(String[] args) throws Exception {
		Class.forName("com.mysql.jdbc.Driver");

		System.out.println("Connecting to a selected database...");
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:8889/MigSchedul_1010611?useSSL=false", "root", "root");
		System.out.println("Connected database successfully...");

		System.out.println("Creating select statement...");
		Statement stmt = conn.createStatement();

		String sql = "SELECT * FROM Products";
		System.out.println("Executing select statement...");
		ResultSet rs = stmt.executeQuery(sql);

		CsvWriterSettings settings = new CsvWriterSettings();
//		CsvWriter writer = new CsvWriter(file, settings);

		System.out.println("Getting metadata...");
		ResultSetMetaData rsm = rs.getMetaData();
		int columnCount = rsm.getColumnCount();
		HashMap<String, List<Integer>> map = new HashMap<>();

		map.put("DataSource_Id", Arrays.asList(1, 56));
		map.put("Products_Key", Arrays.asList(2, 57));
		map.put("Attributes_Name", Arrays.asList(3, 53));
		map.put("Attributes_Value", Arrays.asList(4));
		map.put("Brand", Arrays.asList(5));
		map.put("Classification", Arrays.asList(6));
		map.put("Code", Arrays.asList(7));
		map.put("Company_Id", Arrays.asList(8, 54));
		map.put("Cost", Arrays.asList(9));
		map.put("CreatedDateUtc", Arrays.asList(10));
		map.put("Description", Arrays.asList(11));
		map.put("DisableQuantitySync", Arrays.asList(12));
		map.put("Id", Arrays.asList(13, 55));
		map.put("IncrementalQuantity", Arrays.asList(14));
		map.put("IsAlternateCode", Arrays.asList(15));
		map.put("IsAlternateSKU", Arrays.asList(16));
		map.put("LongDescription", Arrays.asList(17));
		map.put("ModifiedDateUtc", Arrays.asList(18));
		map.put("MOQ", Arrays.asList(19));
		map.put("MOQInfo", Arrays.asList(20));
		map.put("PartNumber", Arrays.asList(21));
		map.put("QuantityAvailable", Arrays.asList(22));
		map.put("QuantityInbound", Arrays.asList(23));
		map.put("QuantityIncoming", Arrays.asList(24));
		map.put("QuantityInStock", Arrays.asList(25));
		map.put("QuantityOnHand", Arrays.asList(26));
		map.put("QuantityOnHold", Arrays.asList(27));
		map.put("QuantityPending", Arrays.asList(28));
		map.put("QuantityPicked", Arrays.asList(29));
		map.put("QuantityTotalFBA", Arrays.asList(30));
		map.put("QuantityTransfer", Arrays.asList(31));
		map.put("ReorderPoint", Arrays.asList(32));
		map.put("RetailPrice", Arrays.asList(33));
		map.put("SalePrice", Arrays.asList(34));
		map.put("ShortDescription", Arrays.asList(35));
		map.put("Sku", Arrays.asList(36));
		map.put("Supplier", Arrays.asList(37));
		map.put("SupplierInfo_Cost", Arrays.asList(38));
		map.put("SupplierInfo_IsActive", Arrays.asList(39));
		map.put("SupplierInfo_IsPrimary", Arrays.asList(40));
		map.put("SupplierInfo_LeadTime", Arrays.asList(41));
		map.put("SupplierInfo_SupplierName", Arrays.asList(42));
		map.put("SupplierInfo_SupplierPartNumber", Arrays.asList(43));
		map.put("VariationParentSku", Arrays.asList(44));
		map.put("WeightUnit", Arrays.asList(45));
		map.put("WeightValue", Arrays.asList(46));
		map.put("Source_Hash_Value", Arrays.asList(47));
		map.put("Mismatch_Flag", Arrays.asList(48));
		map.put("Added_Date", Arrays.asList(49));
		map.put("Added_User", Arrays.asList(50));
		map.put("Updated_Date", Arrays.asList(51));
		map.put("Updated_User", Arrays.asList(52));

		String updateQuery = "UPDATE `Products_test` SET `DataSource_Id`=?, `Products_Key`=?, `Attributes_Name`=?, `Attributes_Value`=?, `Brand`=?, "
		        + "`Classification`=?, `Code`=?, `Company_Id`=?, `Cost`=?, `CreatedDateUtc`=?, `Description`=?, `DisableQuantitySync`=?, `Id`=?, `IncrementalQuantity`=?, "
		        + "`IsAlternateCode`=?, `IsAlternateSKU`=?, `LongDescription`=?, `ModifiedDateUtc`=?, `MOQ`=?, `MOQInfo`=?, `PartNumber`=?, `QuantityAvailable`=?, "
		        + "`QuantityInbound`=?, `QuantityIncoming`=?, `QuantityInStock`=?, `QuantityOnHand`=?, `QuantityOnHold`=?, `QuantityPending`=?, `QuantityPicked`=?, "
		        + "`QuantityTotalFBA`=?, `QuantityTransfer`=?, `ReorderPoint`=?, `RetailPrice`=?, `SalePrice`=?, `ShortDescription`=?, `Sku`=?, `Supplier`=?, "
		        + "`SupplierInfo_Cost`=?, `SupplierInfo_IsActive`=?, `SupplierInfo_IsPrimary`=?, `SupplierInfo_LeadTime`=?, `SupplierInfo_SupplierName`=?, "
		        + "`SupplierInfo_SupplierPartNumber`=?, `VariationParentSku`=?, `WeightUnit`=?, `WeightValue`=?, `Source_Hash_Value`=?, `Mismatch_Flag`=?, "
		        + "`Added_Date`=?, `Added_User`=?, `Updated_Date`=?, `Updated_User`=? WHERE `Attributes_Name`=? AND `Company_Id`=? AND `Id`=? AND `DataSource_Id`=? "
		        + "AND `Products_Key`=?";

		System.out.println("Executing select statement...");
		PreparedStatement updateStatement = conn.prepareStatement(updateQuery);
		int MAX_BATCH_SIZE = 5000;
		int batchSize = 0;
		int batch = 0;

		while (rs.next()) {
			for (int i = 1; i <= columnCount; i++) {
				String column = rsm.getColumnLabel(i);
				Object value = rs.getObject(column);
				List<Integer> keys = map.get(column);
				for (Integer key : keys) {
					updateStatement.setObject(key, value);
				}
			}

			updateStatement.addBatch();
			updateStatement.clearParameters();

			batchSize++;
			if (batchSize == MAX_BATCH_SIZE) {
				batch++;
				System.out.println("Executing batch: " + batch + " - " + new Date());
				batchSize = 0;
				updateStatement.executeBatch();
				updateStatement.clearBatch();
				System.out.println("Executed batch: " + batch + " - " + new Date());
			}
		}
//		rs.getObject("Company_Id");
//		stmt.executeUpdate(
//		        "UPDATE IL_Product SET Updated_Date=CURRENT_TIMESTAMP WHERE Product_Id='02A0009-01' AND Company_Id='unknown' AND DataSource_Id='unknown' AND Product_Key='268'");
//		rs.next();
//		rs.getObject("Company_Id");
	}

}
