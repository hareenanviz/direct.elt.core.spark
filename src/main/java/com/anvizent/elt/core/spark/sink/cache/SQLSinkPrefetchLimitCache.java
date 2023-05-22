package com.anvizent.elt.core.spark.sink.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
public class SQLSinkPrefetchLimitCache extends SQLSinkCache {

	private static final long serialVersionUID = 1L;

	private Connection connection;
	private int batchSize;
	private int selectIndex;
	private boolean removeOnceUsed;
	private PreparedStatement bulkSelectUnionSingleSelect;

	public SQLSinkPrefetchLimitCache(String name, RDBMSConnection rdbmsConnection, String table, ArrayList<String> keys, ArrayList<String> keyColumns,
	        ArrayList<String> selectColumns, int batchSize, boolean removeOnceUsed, boolean keyFieldsCaseSensitive, Integer maxElementsInMemory)
	        throws ImproperValidationException, SQLException {
		super(name, rdbmsConnection, table, keys, keyColumns, selectColumns, keyFieldsCaseSensitive, maxElementsInMemory);

		this.batchSize = batchSize;
		this.removeOnceUsed = removeOnceUsed;

		buildSelectQueries();
	}

	@Override
	@SuppressWarnings({ "deprecation", "unchecked" })
	public RecordFromCache getSelectValues(HashMap<String, Object> row) throws ImproperValidationException, SQLException {
		ArrayList<Object> key = getKey(row);
		Element element;

		if (removeOnceUsed) {
			element = removeAndReturnElement(key);
		} else {
			element = get(key);
		}

		if (element != null) {
			ArrayList<Object> value = (ArrayList<Object>) element.getValue();
			return new RecordFromCache(true, false, value);
		} else {
			return fetch(key);
		}
	}

	private RecordFromCache fetch(ArrayList<Object> key) throws ImproperValidationException, SQLException {
		if (connection.isClosed() || bulkSelectUnionSingleSelect.isClosed()) {
			buildSelectQueries();
		}

		int parameterIndex = 0;
		bulkSelectUnionSingleSelect.setObject(++parameterIndex, selectIndex);

		for (Object object : key) {
			if (object instanceof String && !keyFieldsCaseSensitive) {
				bulkSelectUnionSingleSelect.setObject(++parameterIndex, ((String) object).toUpperCase());
			} else {
				bulkSelectUnionSingleSelect.setObject(++parameterIndex, object);
			}
		}

		System.out.println("SQLSinkCache fetching records using: " + bulkSelectUnionSingleSelect);

		ResultSet resultSet = bulkSelectUnionSingleSelect.executeQuery();

		selectIndex += batchSize;

		RecordFromCache recordFromCache = new RecordFromCache();
		recordFromCache.setValue(new ArrayList<>(0));

		while (resultSet.next()) {
			ArrayList<Object> keyFromDB = getValues(resultSet, keyColumns, keyFieldsCaseSensitive);
			ArrayList<Object> valueFromDB = getValues(resultSet, selectColumns, true);

			if (key.equals(keyFromDB)) {
				recordFromCache.setValue(valueFromDB);
				recordFromCache.setFound(true);
				if (removeOnceUsed) {
					continue;
				}
			}

			Element element = new Element(keyFromDB, valueFromDB);
			put(element);
		}

		return recordFromCache;
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

	private void buildSelectQueries() throws ImproperValidationException, SQLException {
		String singleSelectQuery = "SELECT " + getColumns(selectColumns, keyColumns) + " FROM `" + table + "` WHERE " + getWhereClause(keyColumns);
		String bulkSelectQuery = "SELECT " + getColumns(selectColumns, keyColumns) + " FROM `" + table + "` LIMIT ?," + batchSize;

		connection = getConnection();
		this.bulkSelectUnionSingleSelect = connection.prepareStatement('(' + bulkSelectQuery + ") UNION (" + singleSelectQuery + ')');
	}

	private String getWhereClause(ArrayList<String> columns) {
		StringBuilder query = new StringBuilder();

		for (String column : columns) {
			if (query.length() != 0) {
				query.append(" AND ");
			}

			if (!keyFieldsCaseSensitive) {
				query.append("UPPER(");
			}

			query.append('`').append(column).append('`');

			if (!keyFieldsCaseSensitive) {
				query.append(')');
			}

			query.append(" <=> ?");
		}

		return query.toString();
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
