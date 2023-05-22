package com.anvizent.elt.core.spark.sink.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.anvizent.elt.core.lib.constant.Constants;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.constant.Constants.General;

import net.sf.ehcache.Element;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkPrefetchAllCache extends SQLSinkCache {

	private static final long serialVersionUID = 1L;

	public SQLSinkPrefetchAllCache(String name, RDBMSConnection rdbmsConnection, String table, ArrayList<String> keys, ArrayList<String> keyColumns,
	        ArrayList<String> selectColumns, boolean keyFieldsCaseSensitive, Integer maxElementsInMemory) throws ImproperValidationException, SQLException {
		super(name, rdbmsConnection, table, keys, keyColumns, selectColumns, keyFieldsCaseSensitive, maxElementsInMemory);
	}

	@Override
	public RecordFromCache getSelectValues(HashMap<String, Object> row) {
		ArrayList<Object> key = getKey(row);
		Element element = get(key);

		if (element != null) {
			return (RecordFromCache) element.getObjectValue();
		} else {
			return new RecordFromCache(false, false, null);
		}
	}

	private ArrayList<Object> getValues(ResultSet resultSet, ArrayList<String> columns, boolean caseSensitive) throws SQLException {
		ArrayList<Object> key = new ArrayList<>(columns.size());

		for (String keyField : columns) {
			Object value = resultSet.getObject(keyField);
			if (!caseSensitive && (value instanceof String)) {
				key.add(((String) value).toUpperCase());
			} else {
				key.add(value);
			}
		}

		return key;
	}

	private ArrayList<Object> getKey(HashMap<String, Object> row) {
		ArrayList<Object> key = new ArrayList<>(keys.size());

		for (String keyField : keys) {
			if (!keyFieldsCaseSensitive && row.get(keyField) instanceof String) {
				key.add(((String) row.get(keyField)).toUpperCase());
			} else {
				key.add(row.get(keyField));
			}
		}

		return key;
	}

	public void fetchData() throws ImproperValidationException, SQLException {
		String selectQuery = "SELECT " + getColumns(selectColumns, keyColumns) + " FROM `" + table + "`";
		System.out.println("SQLSinkPrefetchAllCache.fetchData: fetching data using query: " + selectQuery);

		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			connection = getConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery(selectQuery);

			while (resultSet.next()) {
				ArrayList<Object> keyFromDB = getValues(resultSet, keyColumns, keyFieldsCaseSensitive);
				ArrayList<Object> valueFromDB = getValues(resultSet, selectColumns, true);
				RecordFromCache recordFromCache = new RecordFromCache(true, false, valueFromDB);

				put(new Element(keyFromDB, recordFromCache));
			}
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connection != null) {
				connection.close();
			}
		}

	}

	private String getColumns(ArrayList<String> columns, ArrayList<String> columns2) {
		StringBuilder query = new StringBuilder();

		for (String column : columns) {
			if (query.length() != 0) {
				query.append(", ");
			}
			query.append('`').append(column).append('`');
		}

		for (String column : columns2) {
			if (query.length() != 0) {
				query.append(", ");
			}
			query.append('`').append(column).append('`');
		}

		return query.toString();
	}

	private Connection getConnection() throws SQLException, ImproperValidationException {
		try {
			Class.forName(rdbmsConnection.getDriver());

			String jdbcURL = rdbmsConnection.getJdbcURL();
			if (rdbmsConnection.getDriver().equals(Constants.MYSQL_DRIVER)) {
				jdbcURL = rdbmsConnection.checkAndAddParam(jdbcURL, "rewriteBatchedStatements", General.REWRITE_BATCHED_STATEMENTS, "?", "&");
			}

			Connection connection = DriverManager.getConnection(jdbcURL, rdbmsConnection.getUserName(), rdbmsConnection.getPassword());
			connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			return connection;
		} catch (ClassNotFoundException exception) {
			throw new ImproperValidationException(exception);
		}
	}
}
