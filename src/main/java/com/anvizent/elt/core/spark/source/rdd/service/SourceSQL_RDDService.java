package com.anvizent.elt.core.spark.source.rdd.service;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.anvizent.elt.core.lib.constant.JavaTypeVsSparkType;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.spark.resource.config.QuoteConfig;
import com.anvizent.elt.core.spark.source.config.bean.SourceSQLConfigBean;
import com.anvizent.elt.core.spark.source.pojo.OffsetPartition;
import com.anvizent.elt.core.spark.source.resource.config.SQLTableSizeQueries;

public class SourceSQL_RDDService implements Serializable {

	private static final long serialVersionUID = 1L;
	private final OffsetPartition oneRowPartition = new OffsetPartition(-1, 0, 1);

	public OffsetPartition getOneRowPartition() {
		return oneRowPartition;
	}

	public long getTableCount(SourceSQLConfigBean sqlConfigBean) throws RecordProcessingException {
		Connection connection = getConnection(sqlConfigBean.getConnection(), oneRowPartition);
		String query;

		SQLTableSizeQueries sqlTableSizeQueries = sqlConfigBean.getResourceConfig().getTableSizeQuery();
		if (sqlConfigBean.isQuery()) {
			query = getQueryCountQuery(sqlTableSizeQueries.getQuery(), sqlConfigBean.getRawQuery(), sqlConfigBean.getQueryAlias());
		} else {
			QuoteConfig quoteConfig = sqlConfigBean.getResourceConfig().getQuoteConfig().getTable();
			query = getTableCountQuery(sqlTableSizeQueries.getTable(), sqlConfigBean.getTableName(), quoteConfig.getStart(), quoteConfig.getEnd());
		}

		return getCount(query, connection);
	}

	public String getOffSetQuery(SourceSQLConfigBean sqlConfigBean, OffsetPartition partition) {
		String query;

		if (sqlConfigBean.isQuery()) {
			if (sqlConfigBean.isQueryContainsOffset()) {
				query = sqlConfigBean.getResourceConfig().getOffSetQuery().getQueryWithOffSet().getQuery().replace("${query}", sqlConfigBean.getRawQuery())
				        .replace("${alias}", sqlConfigBean.getRawQuery());
			} else {
				query = sqlConfigBean.getResourceConfig().getOffSetQuery().getQueryWithoutOffSet().getQuery().replace("${query}", sqlConfigBean.getRawQuery());
			}
		} else {
			QuoteConfig quoteConfig = sqlConfigBean.getResourceConfig().getQuoteConfig().getTable();
			query = sqlConfigBean.getResourceConfig().getOffSetQuery().getTable().getQuery().replace("${table}",
			        quoteConfig.getStart() + sqlConfigBean.getTableName() + quoteConfig.getEnd());
		}

		return query.replace("${offset}", "" + partition.getOffset()).replace("${size}", "" + partition.getSize());
	}

	public StructType getStructType(SourceSQLConfigBean sqlConfigBean) throws RecordProcessingException, UnimplementedException, UnsupportedException {
		String query = getOffSetQuery(sqlConfigBean, oneRowPartition);
		Connection connection = getConnection(sqlConfigBean.getConnection(), oneRowPartition);

		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			ResultSetMetaData metaData = resultSet.getMetaData();

			return getStructType(metaData);
		} catch (SQLException | ClassNotFoundException exception) {
			throw new RecordProcessingException(exception);
		} finally {
			try {
				if (resultSet != null && !resultSet.isClosed()) {
					resultSet.close();
				}

				if (statement != null && !statement.isClosed()) {
					statement.close();
				}
			} catch (SQLException exception) {
				throw new RecordProcessingException(exception);
			}
		}
	}

	private StructType getStructType(ResultSetMetaData metaData) throws SQLException, UnimplementedException, UnsupportedException, ClassNotFoundException {
		StructField[] fields = new StructField[metaData.getColumnCount()];

		for (int i = 1; i <= fields.length; i++) {
			JavaTypeVsSparkType sparkTypeVsJavaType = JavaTypeVsSparkType.getInstance(Class.forName(metaData.getColumnClassName(i)));
			fields[i - 1] = DataTypes.createStructField(metaData.getColumnLabel(i), sparkTypeVsJavaType.getDataType(), true);
		}

		return new StructType(fields);
	}

	public LinkedList<HashMap<String, Object>> getRows(SourceSQLConfigBean sqlConfigBean, OffsetPartition partition) throws RecordProcessingException {
		if (partition.getSize() == 0 && partition.getOffset() == 0) {
			return new LinkedList<>();
		}

		RecordProcessingException recordProcessingException = null;
		int i = 0;

		do {
			try {
				String query = getOffSetQuery(sqlConfigBean, partition);

				return getRows(sqlConfigBean, partition, query);
			} catch (RecordProcessingException processingException) {
				recordProcessingException = processingException;

				if (sqlConfigBean.getRetryDelay() != null) {
					try {
						Thread.sleep(sqlConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			}
		} while (++i < sqlConfigBean.getMaxRetryCount());

		throw new RuntimeException(recordProcessingException.getMessage(), recordProcessingException);
	}

	public LinkedList<HashMap<String, Object>> getRows(SourceSQLConfigBean sqlConfigBean, OffsetPartition partition, String query)
	        throws RecordProcessingException {
		Connection connection = getConnection(sqlConfigBean.getConnection(), partition);

		Statement statement = null;
		ResultSet resultSet = null;

		try {
			LinkedList<HashMap<String, Object>> rows = new LinkedList<>();
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			ResultSetMetaData metaData = resultSet.getMetaData();
			while (resultSet.next()) {
				rows.add(getRow(resultSet, metaData));
			}

			return rows;
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception);
		} finally {
			try {
				if (resultSet != null && !resultSet.isClosed()) {
					resultSet.close();
				}

				if (statement != null && !statement.isClosed()) {
					statement.close();
				}
			} catch (SQLException exception) {
				throw new RecordProcessingException(exception);
			}
		}
	}

	private HashMap<String, Object> getRow(ResultSet resultSet, ResultSetMetaData metaData) throws SQLException {
		HashMap<String, Object> row = new HashMap<>();

		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			String column = metaData.getColumnLabel(i);
			row.put(column, resultSet.getObject(i));
		}

		return row;
	}

	private long getCount(String query, Connection connection) throws RecordProcessingException {
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			if (resultSet.next()) {
				return resultSet.getLong(1);
			} else {
				return 0l;
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception);
		} finally {
			try {
				if (resultSet != null && !resultSet.isClosed()) {
					resultSet.close();
				}

				if (statement != null && !statement.isClosed()) {
					statement.close();
				}
			} catch (SQLException exception) {
				throw new RecordProcessingException(exception);
			}
		}
	}

	private String getTableCountQuery(String query, String tableName, String quoteStart, String quoteEnd) {
		return query.replace("${table}", quoteStart + tableName + quoteEnd);
	}

	private String getQueryCountQuery(String query, String customQuery, String queryAsTableName) {
		return query.replace("${query}", customQuery).replace("${alias}", queryAsTableName);
	}

	public Connection getConnection(RDBMSConnection connection, OffsetPartition partition) throws RecordProcessingException {
		try {
			return (Connection) ApplicationConnectionBean.getInstance().get(new RDBMSConnectionByTaskId(connection, null, partition.getIndex()), true)[0];
		} catch (ImproperValidationException | UnimplementedException | SQLException | TimeoutException exception) {
			throw new RecordProcessingException(exception);
		}
	}

}
