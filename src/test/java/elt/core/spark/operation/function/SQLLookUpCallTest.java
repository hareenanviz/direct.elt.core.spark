package elt.core.spark.operation.function;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.operation.config.bean.SQLLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.function.SQLLookUpFunction;
import com.anvizent.encryptor.AnvizentEncryptor;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLLookUpCallTest {

	private SQLLookUpFunction sqlLookUpFunction;
	private LinkedHashMap<String, Object> row;

	@Before
	public void init() throws UnsupportedEncodingException, Exception {
		try {
			SQLLookUpConfigBean lookUp = new SQLLookUpConfigBean();
			lookUp.setSelectColumns(new ArrayList<String>() {
				private static final long serialVersionUID = 1L;
				{
					add("id");
					add("company");
					add("work_order");
				}
			});
			lookUp.setSelectFieldAliases(new ArrayList<String>() {
				private static final long serialVersionUID = 1L;
				{
					add("_id");
					add("_company_name");
					add("_work_order_num");
				}
			});
			lookUp.setSelectFieldPositions(new ArrayList<Integer>() {
				private static final long serialVersionUID = 1L;
				{
					add(0);
					add(2);
					add(1);
				}
			});

			lookUp.setWhereFields(new ArrayList<String>() {
				private static final long serialVersionUID = 1L;
				{
					add("key3");
					add("key4");
				}
			});
			lookUp.setWhereColumns(new ArrayList<String>() {
				private static final long serialVersionUID = 1L;
				{
					add("country");
					add("order_number");
				}
			});
			lookUp.setInsertValues(new ArrayList<String>() {
				private static final long serialVersionUID = 1L;
				{
					add("5004");
					add("IND");
					add("111115");
				}
			});
			lookUp.setLimitTo1(true);
			lookUp.setOnZeroFetch(OnZeroFetchOperation.INSERT);
			lookUp.setTableName("test_lookup");
			lookUp.setName("sql lookup");
			lookUp.setCacheType(CacheType.NONE);
			lookUp.setTimeToIdleSeconds(20L);
			lookUp.setMaxElementsInMemory(100);

			RDBMSConnection rdbmsConnection = new RDBMSConnection();
			rdbmsConnection.setDriver("com.mysql.jdbc.Driver");
			rdbmsConnection.setJdbcUrl("jdbc:mysql://115.111.89.73:3306/anvizent_elt_tester_input");
			rdbmsConnection.setUserName(new AnvizentEncryptor("Anvizent", "ELT").decrypt("eA_JvAUKv-9hUnRkDUDVtg"));
			rdbmsConnection.setPassword(new AnvizentEncryptor("Anvizent", "ELT").decrypt("wuzyF5x4RyKY1JRwcGkZ7w"));

			lookUp.setRdbmsConnection(rdbmsConnection);

			lookUp.setSelectQuery(
			        "SELECT  id AS _id, company AS _company_name, work_order AS _work_order_num FROM test_lookup WHERE country = ? AND order_number = ?");
			lookUp.setInsertQuery("INSERT INTO test_lookup(id, company, work_order, country, order_number) VALUES(?, ?, ?, ?, ?)");

			sqlLookUpFunction = new SQLLookUpFunction(lookUp, new LinkedHashMap<>(), new LinkedHashMap<>(), null, null, null);

			row = new LinkedHashMap<>();
			row.put("key1", new Date());
			row.put("key2", new Date());
			row.put("key3", "UNITED STATES");
			row.put("key4", 23115659);
			row.put("key5", 105.5);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Test
	public void renameCallTest() throws Exception {
		try {
			System.out.println(sqlLookUpFunction.call(row));
		} catch (Exception e) {
			// TODO: handle exception
		}
		Assert.assertTrue(true);
	}

}
