package com.anvizent.elt.core.spark.sink.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.common.util.QueryInfo;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.SQLSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkService {

	public static void setFields(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		LinkedHashMap<String, AnvizentDataType> structureAfterRemovingDeleteIndicator = new LinkedHashMap<String, AnvizentDataType>(structure);

		if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteIndicatorField())) {
			structureAfterRemovingDeleteIndicator.remove(sqlSinkConfigBean.getDeleteIndicatorField());
		}

		setSelectColumnsAndFields(sqlSinkConfigBean, structureAfterRemovingDeleteIndicator);
		setInsertFields(sqlSinkConfigBean, structureAfterRemovingDeleteIndicator);
		setUpdateFields(sqlSinkConfigBean, structureAfterRemovingDeleteIndicator);
		setRowFields(sqlSinkConfigBean, structureAfterRemovingDeleteIndicator);
		setRowKeysAndFields(sqlSinkConfigBean, structureAfterRemovingDeleteIndicator);
	}

	private static void setRowKeysAndFields(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		ArrayList<String> rowKeysAndFields = new ArrayList<>(structure.keySet());
		sqlSinkConfigBean.setRowKeysAndFields(rowKeysAndFields);
	}

	private static void setRowFields(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		ArrayList<String> rowFields = new ArrayList<>(structure.keySet());
		if (sqlSinkConfigBean.getKeyFields() != null) {
			rowFields.removeAll(sqlSinkConfigBean.getKeyFields());
		}
		sqlSinkConfigBean.setRowFields(rowFields);
	}

	private static void setInsertFields(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		ArrayList<String> insertFields = SQLUpdateService.getDifferFieldsAndKeys(new ArrayList<>(structure.keySet()), sqlSinkConfigBean.getKeyFields(),
		        sqlSinkConfigBean.getKeyColumns(), sqlSinkConfigBean.getFieldsDifferToColumns(), sqlSinkConfigBean.getColumnsDifferToFields());
		sqlSinkConfigBean.setInsertFields(insertFields);
	}

	private static void setUpdateFields(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		ArrayList<String> updateFields = SQLUpdateService.getDifferFieldsAndKeys(new ArrayList<>(structure.keySet()), sqlSinkConfigBean.getKeyFields(),
		        sqlSinkConfigBean.getKeyColumns(), sqlSinkConfigBean.getFieldsDifferToColumns(), sqlSinkConfigBean.getColumnsDifferToFields());
		if (updateFields != null && !updateFields.isEmpty()) {
			if (sqlSinkConfigBean.getKeyColumns() == null || sqlSinkConfigBean.getKeyColumns().isEmpty()) {
				if (sqlSinkConfigBean.getKeyFields() != null && !sqlSinkConfigBean.getKeyFields().isEmpty()) {
					updateFields.removeAll(sqlSinkConfigBean.getKeyFields());
				}
			} else {
				updateFields.removeAll(sqlSinkConfigBean.getKeyColumns());
			}
		}

		sqlSinkConfigBean.setUpdateFields(updateFields);
	}

	private static void setSelectColumnsAndFields(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {
		ArrayList<String> selectColumns;
		ArrayList<String> selectFields;

		if (sqlSinkConfigBean.getChecksumField() != null && !sqlSinkConfigBean.getChecksumField().isEmpty()) {
			selectColumns = new ArrayList<>(1);
			selectFields = new ArrayList<>(1);

			if (sqlSinkConfigBean.getFieldsDifferToColumns() != null && !sqlSinkConfigBean.getFieldsDifferToColumns().isEmpty()) {
				int index = sqlSinkConfigBean.getFieldsDifferToColumns().indexOf(sqlSinkConfigBean.getChecksumField());
				selectColumns.add(sqlSinkConfigBean.getColumnsDifferToFields().get(index));
			} else {
				selectColumns.add(sqlSinkConfigBean.getChecksumField());
			}

			selectFields.add(sqlSinkConfigBean.getChecksumField());
		} else if (sqlSinkConfigBean.isAlwaysUpdate()) {
			selectColumns = new ArrayList<>(0);
			selectFields = new ArrayList<>(0);
		} else if (CollectionUtils.isNotEmpty(sqlSinkConfigBean.getMetaDataFields())) {
			selectFields = new ArrayList<>(structure.keySet().size() - sqlSinkConfigBean.getKeyFields().size());
			selectFields.addAll(structure.keySet());
			selectFields.removeAll(sqlSinkConfigBean.getKeyFields());
			selectFields.removeAll(sqlSinkConfigBean.getMetaDataFields());

			selectColumns = SQLUpdateService.getDifferFields(selectFields, sqlSinkConfigBean.getFieldsDifferToColumns(),
			        sqlSinkConfigBean.getColumnsDifferToFields());
		} else {
			if (sqlSinkConfigBean.getKeyFields() == null || sqlSinkConfigBean.getKeyFields().isEmpty()) {
				selectFields = new ArrayList<>(structure.keySet());
			} else {
				selectFields = new ArrayList<>(structure.keySet().size() - sqlSinkConfigBean.getKeyFields().size());
				selectFields.addAll(structure.keySet());
				selectFields.removeAll(sqlSinkConfigBean.getKeyFields());
			}

			selectColumns = SQLUpdateService.getDifferFields(selectFields, sqlSinkConfigBean.getFieldsDifferToColumns(),
			        sqlSinkConfigBean.getColumnsDifferToFields());
		}

		sqlSinkConfigBean.setSelectColumns(selectColumns);
		sqlSinkConfigBean.setSelectFields(selectFields);
	}

	@SuppressWarnings("unused")
	public static boolean checkIfTableExists(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws ImproperValidationException, SQLException, UnsupportedException, UnimplementedException, TimeoutException {
		SQLException sqlException = null;
		int i = 0;
		do {
			try {
				sqlException = null;
				boolean tableExists = false;

				DatabaseMetaData databaseMetaData = ((Connection) ApplicationConnectionBean.getInstance()
				        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0]).getMetaData();
				ResultSet resultSet = databaseMetaData.getTables(null, null, sqlSinkConfigBean.getTableName(), null);

				tableExists = resultSet != null && resultSet.next();

				SQLUtil.closeResultSetObject(resultSet);

				return tableExists;
			} catch (SQLException exception) {
				sqlException = exception;
				if (sqlSinkConfigBean.getRetryDelay() != null && sqlSinkConfigBean.getRetryDelay() > 0) {
					try {
						Thread.sleep(sqlSinkConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			}
		} while (++i < sqlSinkConfigBean.getMaxRetryCount());

		if (sqlException != null) {
			throw sqlException;
		}

		return false;
	}

	public static void createTable(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure, String componentName)
	        throws SQLException, UnsupportedException, ImproperValidationException, InvalidConfigException, UnimplementedException, TimeoutException,
	        InvalidInputForConfigException {
		SQLException sqlException = null;
		int i = 0;
		do {
			String query = null;
			try {
				sqlException = null;

				Connection connection = (Connection) ApplicationConnectionBean.getInstance()
				        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0];
				validateDBConstantTypes(sqlSinkConfigBean, componentName);

				LinkedHashMap<String, AnvizentDataType> metaStructure = getMetaStructure(structure, sqlSinkConfigBean);

				LinkedHashSet<String> constantFields = new LinkedHashSet<>();
				addAllConstantFields(constantFields, sqlSinkConfigBean);
				ArrayList<String> uniqueConstantFields = new ArrayList<>(constantFields);

				ArrayList<String> constantTypes = new ArrayList<>();
				addAllConstantTypes(constantTypes, sqlSinkConfigBean, uniqueConstantFields);

				query = SQLUtil.buildCreateTableQuery(sqlSinkConfigBean.getTableName(), sqlSinkConfigBean.getRdbmsConnection().getDriver(),
				        sqlSinkConfigBean.getKeyColumns() == null || sqlSinkConfigBean.getKeyColumns().isEmpty() ? sqlSinkConfigBean.getKeyFields()
				                : sqlSinkConfigBean.getKeyColumns(),
				        metaStructure, new ArrayList<>(constantFields), constantTypes);
				PreparedStatement preparedStatement = connection.prepareStatement(query);

				if (preparedStatement.executeUpdate() < 0) {
					throw new SQLException("Failed to create table: '" + sqlSinkConfigBean.getTableName() + "'");
				}

				SQLUtil.closePreparedStatementObject(preparedStatement);

				return;
			} catch (SQLSyntaxErrorException exception) {
				throw new SQLSyntaxErrorException("Invalid query: " + query, exception);
			} catch (SQLException exception) {
				sqlException = exception;

				if (sqlSinkConfigBean.getRetryDelay() != null && sqlSinkConfigBean.getRetryDelay() > 0) {
					try {
						Thread.sleep(sqlSinkConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			}
		} while (++i < sqlSinkConfigBean.getMaxRetryCount());

		if (sqlException != null) {
			throw sqlException;
		}
	}

	private static void validateDBConstantTypes(SQLSinkConfigBean sqlSinkConfigBean, String componentName) throws InvalidConfigException {
		validateDBConstantTypes(componentName, sqlSinkConfigBean.getName(), sqlSinkConfigBean.getSeekDetails(), sqlSinkConfigBean.getDBInsertMode(),
		        sqlSinkConfigBean.getConstantsConfigBean().getColumns(), sqlSinkConfigBean.getConstantsConfigBean().getTypes(),
		        sqlSinkConfigBean.getTableName(), SQLSink.CONSTANT_STORE_TYPES, false);

		validateDBConstantTypes(componentName, sqlSinkConfigBean.getName(), sqlSinkConfigBean.getSeekDetails(), sqlSinkConfigBean.getDBInsertMode(),
		        sqlSinkConfigBean.getInsertConstantsConfigBean().getColumns(), sqlSinkConfigBean.getInsertConstantsConfigBean().getTypes(),
		        sqlSinkConfigBean.getTableName(), SQLSink.INSERT_CONSTANT_STORE_TYPES, true);

		validateDBConstantTypes(componentName, sqlSinkConfigBean.getName(), sqlSinkConfigBean.getSeekDetails(), sqlSinkConfigBean.getDBInsertMode(),
		        sqlSinkConfigBean.getUpdateConstantsConfigBean().getColumns(), sqlSinkConfigBean.getUpdateConstantsConfigBean().getTypes(),
		        sqlSinkConfigBean.getTableName(), SQLSink.UPDATE_CONSTANT_STORE_TYPES, true);
	}

	private static void validateDBConstantTypes(String componentName, String componentConfigName, SeekDetails seekDetails, DBInsertMode dbInsertMode,
	        ArrayList<String> constantFields, ArrayList<String> constantTypes, String tableName, String constantTypesConfig, boolean applicable)
	        throws InvalidConfigException {
		if ((!applicable && !dbInsertMode.equals(DBInsertMode.UPSERT)) || (applicable && dbInsertMode.equals(DBInsertMode.UPSERT))) {
			if ((constantFields != null && !constantFields.isEmpty()) && (constantTypes == null || constantTypes.isEmpty())) {
				InvalidConfigException exception = new InvalidConfigException();
				exception.setComponent(componentName);
				exception.setComponentName(componentConfigName);
				exception.setSeekDetails(seekDetails);

				exception.add(Message.SINGLE_KEY_MANDATORY_WHEN_TABLE_DOESNT_EXISTS, constantTypesConfig, tableName);

				throw exception;
			}
		}
	}

	private static void addAllConstantTypes(ArrayList<String> constantTypes, SQLSinkConfigBean sqlSinkConfigBean, ArrayList<String> uniqueConstantFields) {
		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			if (sqlSinkConfigBean.getInsertConstantsConfigBean().getTypes() != null && !sqlSinkConfigBean.getInsertConstantsConfigBean().getTypes().isEmpty()) {
				addConstantTypes(sqlSinkConfigBean.getInsertConstantsConfigBean().getColumns(), sqlSinkConfigBean.getInsertConstantsConfigBean().getTypes(),
				        uniqueConstantFields, constantTypes);
			}

			if (sqlSinkConfigBean.getUpdateConstantsConfigBean().getTypes() != null && !sqlSinkConfigBean.getUpdateConstantsConfigBean().getTypes().isEmpty()) {
				addConstantTypes(sqlSinkConfigBean.getUpdateConstantsConfigBean().getColumns(), sqlSinkConfigBean.getUpdateConstantsConfigBean().getTypes(),
				        uniqueConstantFields, constantTypes);
			}
		} else if (sqlSinkConfigBean.getConstantsConfigBean().getTypes() != null && !sqlSinkConfigBean.getConstantsConfigBean().getTypes().isEmpty()) {
			constantTypes.addAll(sqlSinkConfigBean.getConstantsConfigBean().getTypes());
		}
	}

	private static void addAllConstantFields(Set<String> constantFields, SQLSinkConfigBean sqlSinkConfigBean) {
		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			if (sqlSinkConfigBean.getInsertConstantsConfigBean().getColumns() != null
			        && !sqlSinkConfigBean.getInsertConstantsConfigBean().getColumns().isEmpty()) {
				constantFields.addAll(sqlSinkConfigBean.getInsertConstantsConfigBean().getColumns());
			}

			if (sqlSinkConfigBean.getUpdateConstantsConfigBean().getColumns() != null
			        && !sqlSinkConfigBean.getUpdateConstantsConfigBean().getColumns().isEmpty()) {
				constantFields.addAll(sqlSinkConfigBean.getUpdateConstantsConfigBean().getColumns());
			}
		} else if (sqlSinkConfigBean.getConstantsConfigBean().getColumns() != null && !sqlSinkConfigBean.getConstantsConfigBean().getColumns().isEmpty()) {
			constantFields.addAll(sqlSinkConfigBean.getConstantsConfigBean().getColumns());
		}
	}

	private static void addConstantTypes(ArrayList<String> constantFields, ArrayList<String> constantTypes, ArrayList<String> uniqueConstantFields,
	        ArrayList<String> uniqueConstantTypes) {

		for (int i = 0; i < uniqueConstantFields.size(); i++) {
			int insertIndex = constantFields.indexOf(uniqueConstantFields.get(i));
			if (insertIndex != -1) {
				uniqueConstantTypes.add(constantTypes.get(insertIndex));
			}
		}

		uniqueConstantFields.removeAll(constantFields);
	}

	private static LinkedHashMap<String, AnvizentDataType> getMetaStructure(LinkedHashMap<String, AnvizentDataType> structure,
	        SQLSinkConfigBean sqlSinkConfigBean) {
		LinkedHashMap<String, AnvizentDataType> metaStructure = new LinkedHashMap<>();
		ArrayList<String> keyFields = sqlSinkConfigBean.getKeyFields();
		ArrayList<String> keyColumns = sqlSinkConfigBean.getKeyColumns();

		if (keyFields != null && keyColumns != null) {
			for (Entry<String, AnvizentDataType> entry : structure.entrySet()) {
				String key = entry.getKey();

				if (keyFields.contains(key)) {
					int index = keyFields.indexOf(key);
					metaStructure.put(keyColumns.get(index), entry.getValue());
				} else {
					metaStructure.put(key, entry.getValue());
				}
			}
		} else {
			metaStructure.putAll(structure);
		}

		return getMetaStructure(sqlSinkConfigBean, metaStructure);
	}

	private static LinkedHashMap<String, AnvizentDataType> getMetaStructure(SQLSinkConfigBean sqlSinkConfigBean,
	        LinkedHashMap<String, AnvizentDataType> structure) {
		ArrayList<String> fieldsDifferToColumns = sqlSinkConfigBean.getFieldsDifferToColumns();
		ArrayList<String> columnsDifferToFields = sqlSinkConfigBean.getColumnsDifferToFields();

		if (columnsDifferToFields != null && fieldsDifferToColumns != null) {
			LinkedHashMap<String, AnvizentDataType> metaStructure = new LinkedHashMap<>();

			for (Entry<String, AnvizentDataType> entry : structure.entrySet()) {
				String key = entry.getKey();

				int index = fieldsDifferToColumns.indexOf(key);
				if (index != -1) {
					metaStructure.put(columnsDifferToFields.get(index), entry.getValue());
				} else {
					metaStructure.put(key, entry.getValue());
				}
			}

			return metaStructure;
		} else {
			return structure;
		}
	}

	public static void dropTable(SQLSinkConfigBean sqlSinkConfigBean)
	        throws ImproperValidationException, SQLException, UnsupportedException, UnimplementedException, TimeoutException, InvalidInputForConfigException {
		SQLException sqlException = null;
		int i = 0;
		do {
			try {
				sqlException = null;
				PreparedStatement preparedStatement = ((Connection) ApplicationConnectionBean.getInstance()
				        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0])
				                .prepareStatement(
				                        SQLUtil.buildDropTableQuery(sqlSinkConfigBean.getTableName(), sqlSinkConfigBean.getRdbmsConnection().getDriver()));
				preparedStatement.executeUpdate();

				SQLUtil.closePreparedStatementObject(preparedStatement);

				return;
			} catch (SQLException exception) {
				sqlException = exception;
				if (sqlSinkConfigBean.getRetryDelay() != null && sqlSinkConfigBean.getRetryDelay() > 0) {
					try {
						Thread.sleep(sqlSinkConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			}
		} while (++i < sqlSinkConfigBean.getMaxRetryCount());

		if (sqlException != null) {
			throw sqlException;
		}
	}

	public static void truncate(SQLSinkConfigBean sqlSinkConfigBean)
	        throws ImproperValidationException, SQLException, UnsupportedException, UnimplementedException, TimeoutException, InvalidInputForConfigException {
		SQLException sqlException = null;
		int i = 0;
		do {
			try {
				sqlException = null;
				PreparedStatement preparedStatement = ((Connection) ApplicationConnectionBean.getInstance()
				        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0])
				                .prepareStatement(
				                        SQLUtil.truncateTableQuery(sqlSinkConfigBean.getTableName(), sqlSinkConfigBean.getRdbmsConnection().getDriver()));
				preparedStatement.executeUpdate();

				SQLUtil.closePreparedStatementObject(preparedStatement);

				return;
			} catch (SQLException exception) {
				sqlException = exception;
				if (sqlSinkConfigBean.getRetryDelay() != null && sqlSinkConfigBean.getRetryDelay() > 0) {
					try {
						Thread.sleep(sqlSinkConfigBean.getRetryDelay());
					} catch (InterruptedException e) {
					}
				}
			}
		} while (++i < sqlSinkConfigBean.getMaxRetryCount());

		if (sqlException != null) {
			throw sqlException;
		}
	}

	public static void buildSelectQuery(SQLSinkConfigBean sqlSinkConfigBean, ArrayList<String> rowKeySet)
	        throws UnimplementedException, InvalidInputForConfigException {
		ArrayList<String> whereColumns;

		if (sqlSinkConfigBean.getKeyColumns() == null || sqlSinkConfigBean.getKeyColumns().isEmpty()) {
			whereColumns = sqlSinkConfigBean.getKeyFields();
		} else {
			whereColumns = sqlSinkConfigBean.getKeyColumns();
		}

		sqlSinkConfigBean
		        .setSelectQuery(SQLUtil.buildSelectQueryWithWhereColumns(sqlSinkConfigBean.getRdbmsConnection().getDriver(), sqlSinkConfigBean.getTableName(),
		                sqlSinkConfigBean.getSelectColumns(), null, whereColumns, null, null, sqlSinkConfigBean.isKeyFieldsCaseSensitive()));
	}

	public static void buildSelectAllQuery(SQLSinkConfigBean sqlSinkConfigBean, ArrayList<String> rowKeySet)
	        throws UnimplementedException, InvalidInputForConfigException {
		ArrayList<String> whereColumns;

		if (sqlSinkConfigBean.getKeyColumns() == null || sqlSinkConfigBean.getKeyColumns().isEmpty()) {
			whereColumns = sqlSinkConfigBean.getKeyFields();
		} else {
			whereColumns = sqlSinkConfigBean.getKeyColumns();
		}

		ArrayList<String> selectFields = new ArrayList<>(sqlSinkConfigBean.getSelectColumns());
		selectFields.addAll(whereColumns);

		sqlSinkConfigBean.setSelectAllQuery(
		        SQLUtil.buildSelectQuery(sqlSinkConfigBean.getRdbmsConnection().getDriver(), sqlSinkConfigBean.getTableName(), selectFields, null, null, null));
	}

	public static void buildInsertQuery(SQLSinkConfigBean sqlSinkConfigBean) throws UnimplementedException, InvalidInputForConfigException {
		buildInsertQuery(sqlSinkConfigBean, sqlSinkConfigBean.getConstantsConfigBean().getColumns(), sqlSinkConfigBean.getConstantsConfigBean().getValues());
	}

	public static void buildInsertQuery(SQLSinkConfigBean sqlSinkConfigBean, ArrayList<String> constantFields, ArrayList<String> constantValues)
	        throws UnimplementedException, InvalidInputForConfigException {
		sqlSinkConfigBean.setInsertQuery(SQLUtil.buildInsertQueryWithConstantFields(sqlSinkConfigBean.getRdbmsConnection().getDriver(),
		        sqlSinkConfigBean.getInsertFields(), sqlSinkConfigBean.getTableName(), constantFields, constantValues));
	}

	public static void buildUpdateQuery(SQLSinkConfigBean sqlSinkConfigBean) throws UnimplementedException, InvalidInputForConfigException {
		buildUpdateQuery(sqlSinkConfigBean, sqlSinkConfigBean.getConstantsConfigBean().getColumns(), sqlSinkConfigBean.getConstantsConfigBean().getValues());
	}

	private static void buildUpdateQuery(SQLSinkConfigBean sqlSinkConfigBean, ArrayList<String> constantFields, ArrayList<String> constantValues)
	        throws UnimplementedException, InvalidInputForConfigException {
		sqlSinkConfigBean.setUpdateQuery(SQLUtil.buildUpdateQuery(sqlSinkConfigBean.getRdbmsConnection().getDriver(), sqlSinkConfigBean.getUpdateFields(),
		        sqlSinkConfigBean.getTableName(),
		        sqlSinkConfigBean.getKeyColumns() == null || sqlSinkConfigBean.getKeyColumns().isEmpty() ? sqlSinkConfigBean.getKeyFields()
		                : sqlSinkConfigBean.getKeyColumns(),
		        constantFields, constantValues, sqlSinkConfigBean.isKeyFieldsCaseSensitive()));
	}

	public static void buildUpsertQueries(SQLSinkConfigBean sqlSinkConfigBean) throws UnimplementedException, InvalidInputForConfigException {
		buildInsertQuery(sqlSinkConfigBean, sqlSinkConfigBean.getInsertConstantsConfigBean().getColumns(),
		        sqlSinkConfigBean.getInsertConstantsConfigBean().getValues());
		buildUpdateQuery(sqlSinkConfigBean, sqlSinkConfigBean.getUpdateConstantsConfigBean().getColumns(),
		        sqlSinkConfigBean.getUpdateConstantsConfigBean().getValues());
	}

	public static QueryInfo buildDeleteQueries(Set<String> keySet, SQLSinkConfigBean sqlSinkConfigBean) {
		if (StringUtils.isBlank(sqlSinkConfigBean.getDeleteIndicatorField())) {
			return null;
		}

		ArrayList<String> columns;
		if (sqlSinkConfigBean.getKeyFields() != null && !sqlSinkConfigBean.getKeyFields().isEmpty()) {
			if (sqlSinkConfigBean.getKeyColumns() != null && !sqlSinkConfigBean.getKeyColumns().isEmpty()) {
				columns = sqlSinkConfigBean.getKeyColumns();
			} else {
				columns = sqlSinkConfigBean.getKeyFields();
			}
		} else {
			columns = getAllColumns(keySet, sqlSinkConfigBean);
		}

		QueryInfo deleteQuery = new QueryInfo();
		deleteQuery.setQuery(getDeleteQuery(sqlSinkConfigBean.getTableName(), columns, sqlSinkConfigBean.isKeyFieldsCaseSensitive()));
		deleteQuery.setsToSet(columns);

		return deleteQuery;
	}

	private static ArrayList<String> getAllColumns(Set<String> keySet, SQLSinkConfigBean sqlSinkConfigBean) {
		ArrayList<String> columns = new ArrayList<>(keySet);
		columns.remove(sqlSinkConfigBean.getDeleteIndicatorField());
		ArrayList<String> fieldsDifferToColumns = sqlSinkConfigBean.getFieldsDifferToColumns();

		if (fieldsDifferToColumns != null && !fieldsDifferToColumns.isEmpty()) {
			setAllColumnsWithAliases(columns, fieldsDifferToColumns, sqlSinkConfigBean.getColumnsDifferToFields());
		}

		return columns;
	}

	private static void setAllColumnsWithAliases(ArrayList<String> columns, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields) {
		for (int i = 0; i < fieldsDifferToColumns.size(); i++) {
			columns.set(columns.indexOf(fieldsDifferToColumns.get(i)), columnsDifferToFields.get(i));
		}
	}

	private static String getDeleteQuery(String tableName, ArrayList<String> columns, boolean caseSensitive) {
		return "DELETE FROM `" + tableName + "` " + getWhereClause(columns, caseSensitive);
	}

	private static String getWhereClause(ArrayList<String> columns, boolean caseSensitive) {
		StringBuilder where = new StringBuilder();

		if (columns != null && !columns.isEmpty()) {
			for (String column : columns) {
				if (where.length() != 0) {
					where.append(" AND ");
				}

				if (caseSensitive) {
					where.append("BINARY ");
				}

				where.append('`').append(column).append("` <=> ?");
			}
		}

		if (where.length() != 0) {
			return "WHERE " + where.toString();
		} else {
			return where.toString();
		}
	}
}
