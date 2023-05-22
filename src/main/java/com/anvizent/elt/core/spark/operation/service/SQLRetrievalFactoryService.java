package com.anvizent.elt.core.spark.operation.service;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.SQL;
import com.anvizent.elt.core.spark.constant.SQLTypes;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLRetrievalFactoryService {

	public static void setCustomWhereQuery(SQLRetrievalConfigBean sqlRetrievalConfigBean) {
		if (StringUtils.isNotBlank(sqlRetrievalConfigBean.getCustomWhere())) {
			ArrayList<String> whereFields = new ArrayList<String>();

			String customQuery = replaceWhereFields(sqlRetrievalConfigBean.getCustomWhere(), whereFields);
			sqlRetrievalConfigBean.setWhereFields(whereFields);
			sqlRetrievalConfigBean.setCustomQueryAfterProcess(customQuery);
		}
	}

	private static String replaceWhereFields(String query, ArrayList<String> whereFields) {
		String field = StringUtils.substringBetween(query, Constants.General.WHERE_FIELDS_PLACEHOLDER_START, Constants.General.WHERE_FIELDS_PLACEHOLDER_END);
		whereFields.add(field);
		query = StringUtils.replaceOnce(query, Constants.General.WHERE_FIELDS_PLACEHOLDER_START + field + Constants.General.WHERE_FIELDS_PLACEHOLDER_END,
		        SQL.BINDING_PARAM);

		if (query.contains(Constants.General.WHERE_FIELDS_PLACEHOLDER_START)) {
			query = replaceWhereFields(query, whereFields);
		}

		return query;
	}

	public static void buildInsertQuery(SQLRetrievalConfigBean sqlRetrievalConfigBean) throws UnimplementedException, InvalidInputForConfigException {
		sqlRetrievalConfigBean.setInsertQuery(SQLUtil.buildInsertQuery(sqlRetrievalConfigBean.getRdbmsConnection().getDriver(),
		        getInsertFields(sqlRetrievalConfigBean), sqlRetrievalConfigBean.getTableName()));
	}

	private static ArrayList<String> getInsertFields(SQLRetrievalConfigBean sqlRetrievalConfigBean) {
		ArrayList<String> insertFields = new ArrayList<>();
		if (sqlRetrievalConfigBean.getCustomWhere() == null || sqlRetrievalConfigBean.getCustomWhere().isEmpty()) {
			insertFields.addAll(sqlRetrievalConfigBean.getSelectColumns());

			if (sqlRetrievalConfigBean.isUseAIValue() && sqlRetrievalConfigBean.getAiColumnIndex() != null && sqlRetrievalConfigBean.getAiColumnIndex() >= 0) {
				insertFields.remove((int) sqlRetrievalConfigBean.getAiColumnIndex());
			}

			insertFields.addAll(sqlRetrievalConfigBean.getWhereColumns() == null || sqlRetrievalConfigBean.getWhereColumns().isEmpty()
			        ? sqlRetrievalConfigBean.getWhereFields()
			        : sqlRetrievalConfigBean.getWhereColumns());
		}

		return insertFields;
	}

	public static ArrayList<AnvizentDataType> getSelectFieldDataTypes(SQLRetrievalConfigBean sqlRetrievalConfigBean) throws ImproperValidationException,
	        SQLException, ClassNotFoundException, UnsupportedException, UnimplementedException, TimeoutException, InvalidInputForConfigException {
		Connection connection = (Connection) ApplicationConnectionBean.getInstance()
		        .get(new RDBMSConnectionByTaskId(sqlRetrievalConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0];

		PreparedStatement preparedStatement = connection
		        .prepareStatement(SQLUtil.build0RecordsSelectQuery(sqlRetrievalConfigBean.getRdbmsConnection().getDriver(),
		                sqlRetrievalConfigBean.getTableName(), sqlRetrievalConfigBean.getSelectColumns(), sqlRetrievalConfigBean.getSelectFieldAliases()));

		ResultSet resultSet = preparedStatement.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();

		ArrayList<AnvizentDataType> selectFieldDataTypes = new ArrayList<>();
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			selectFieldDataTypes.add(new AnvizentDataType(Class.forName(metaData.getColumnClassName(i)), metaData.getPrecision(i), metaData.getScale(i)));
		}

		SQLUtil.closeConnectionObjects(null, preparedStatement, resultSet);

		return selectFieldDataTypes;
	}

	@SuppressWarnings("rawtypes")
	public static HashMap<String, HashMap<String, ? extends Serializable>> getSQLTypes(RDBMSConnection rdbmsConnection, String tableName,
	        ArrayList<String> selectFields, ArrayList<String> selectFieldAliases)
	        throws ImproperValidationException, SQLException, ClassNotFoundException, UnimplementedException, InvalidInputForConfigException, TimeoutException {

		Connection connection = (Connection) ApplicationConnectionBean.getInstance()
		        .get(new RDBMSConnectionByTaskId(rdbmsConnection, null, TaskContext.getPartitionId()), true)[0];

		PreparedStatement preparedStatement = connection
		        .prepareStatement(SQLUtil.build0RecordsSelectQuery(rdbmsConnection.getDriver(), tableName, selectFields, selectFieldAliases));

		ResultSet resultSet = preparedStatement.executeQuery();
		ResultSetMetaData rsmd = resultSet.getMetaData();
		HashMap<String, SQLTypes> sqlTypes = new HashMap<>();
		HashMap<String, Class> javaTypes = new HashMap<>();
		HashMap<String, String> aiColumn = new HashMap<>();

		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			if (rsmd.isAutoIncrement(i)) {
				aiColumn.put(Constants.General.AI_COLUMN, rsmd.getColumnLabel(i));
			}
			sqlTypes.put(rsmd.getColumnLabel(i), SQLTypes.getInstance(rsmd.getColumnType(i)));
			javaTypes.put(rsmd.getColumnLabel(i), Class.forName(rsmd.getColumnClassName(i)));
		}

		HashMap<String, HashMap<String, ? extends Serializable>> insertColumnsTypes = new HashMap<>();

		insertColumnsTypes.put(Constants.General.SQL_TYPES, sqlTypes);
		insertColumnsTypes.put(Constants.General.JAVA_TYPES, javaTypes);
		insertColumnsTypes.put(Constants.General.AI_COLUMN, aiColumn);

		SQLUtil.closeConnectionObjects(null, preparedStatement, resultSet);

		return insertColumnsTypes;
	}
}
