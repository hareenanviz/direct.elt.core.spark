package com.anvizent.elt.core.spark.common.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.commons.constants.Constants.ELTConstants;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.constant.StoreType;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.listener.common.connection.util.SQLConnectionUtil;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.constant.Constants.SQL;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.sink.config.bean.SQLConnectionByPartition;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLUtil {

	public static void setPreparedStatement(PreparedStatement preparedStatement, HashMap<String, Object> row, ArrayList<String> rowKeys) throws SQLException {
		setPreparedStatement(preparedStatement, row, rowKeys, 1);
	}

	public static void setPreparedStatement(PreparedStatement preparedStatement, HashMap<String, Object> row, ArrayList<String> rowKeys, int startIndex)
	        throws SQLException {
		for (int i = 0; i < rowKeys.size(); i++) {
			preparedStatement.setObject(i + startIndex, row.get(rowKeys.get(i)));
		}
	}

	public static void setPreparedStatement(PreparedStatement preparedStatement, HashMap<String, Object> row, ArrayList<String> rowKeys,
	        Map<String, List<Integer>> fieldsIndexesToSet) throws SQLException {
		for (String rowKey : rowKeys) {
			List<Integer> indexesToSet = fieldsIndexesToSet.get(rowKey);
			for (Integer index : indexesToSet) {
				preparedStatement.setObject(index, row.get(rowKey));
			}
		}
	}

	public static void setPreparedStatement(PreparedStatement preparedStatement, HashMap<String, Object> row, Map<String, List<Integer>> fieldIndexesToSet)
	        throws SQLException {
		for (Entry<String, List<Integer>> fieldIndexesToSetEntry : fieldIndexesToSet.entrySet()) {
			for (Integer index : fieldIndexesToSetEntry.getValue()) {
				preparedStatement.setObject(index, row.get(fieldIndexesToSetEntry.getKey()));
			}
		}
	}

	public static void setPreparedStatement(PreparedStatement preparedStatement, HashMap<String, Object> row, List<String> fieldsToSet) throws SQLException {
		int i = 0;
		for (String field : fieldsToSet) {
			preparedStatement.setObject(++i, row.get(field));
		}
	}

	public static String build0RecordsSelectQuery(String driverName, String tableName, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases)
	        throws UnimplementedException, InvalidInputForConfigException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return build0RecordsSelectQuery(tableName, selectFields, selectFieldAliases, startMetaCharacter, endMetaCharacter);
		}
	}

	private static String build0RecordsSelectQuery(String tableName, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases,
	        char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.SELECT);

		buildSelectQuery(sBuilder, selectFields, selectFieldAliases, startMetaCharacter, endMetaCharacter);

		sBuilder.append(
		        " " + SQL.FROM + startMetaCharacter + tableName + endMetaCharacter + " " + SQL.WHERE + "1" + " " + General.Operator.NOT_EQUAL_TO + " " + "1");

		return sBuilder.toString();
	}

	private static void buildSelectQuery(StringBuilder sBuilder, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases, char startMetaCharacter,
	        char endMetaCharacter) {
		if (selectFields.size() == 0) {
			sBuilder.append("1 ");
			return;
		} else {
			for (int i = 0; i < selectFields.size(); i++) {
				sBuilder.append(startMetaCharacter + selectFields.get(i) + endMetaCharacter);

				if (selectFieldAliases != null && !selectFieldAliases.isEmpty() && StringUtils.isNotBlank(selectFieldAliases.get(i))) {
					sBuilder.append(" " + SQL.AS + startMetaCharacter + selectFieldAliases.get(i) + endMetaCharacter);
				}

				if (i < selectFields.size() - 1) {
					sBuilder.append(SQL.COMMA);
				}
			}

			if (selectFields.size() == 1) {
				sBuilder.append(" ");
			}
		}
	}

	public static Connection getConnection(RDBMSConnection rdbmsConnection) throws SQLException, ImproperValidationException {
		return SQLConnectionUtil.getConnection(rdbmsConnection);
	}

	public static Connection getConnection(String jdbcUrl, String userName, String password, String driver) throws SQLException, ImproperValidationException {
		return SQLConnectionUtil.getConnection(jdbcUrl, userName, password, driver);
	}

	public static String buildSelectQueryWithCustomWhere(String driverName, String tableName, ArrayList<String> selectFields,
	        ArrayList<String> selectFieldAliases, String customWhereQuery, ArrayList<String> orderBy, ArrayList<String> orderByTypes,
	        boolean keyFieldsCaseSensitive) throws UnimplementedException, InvalidInputForConfigException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildSelectQueryWithCustomWhere(driverName, tableName, selectFields, selectFieldAliases, customWhereQuery, orderBy, orderByTypes,
			        keyFieldsCaseSensitive, startMetaCharacter, endMetaCharacter);
		}
	}

	private static String buildSelectQueryWithCustomWhere(String driverName, String tableName, ArrayList<String> selectFields,
	        ArrayList<String> selectFieldAliases, String customWhereQuery, ArrayList<String> orderBy, ArrayList<String> orderByTypes,
	        boolean keyFieldsCaseSensitive, char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.SELECT);

		buildSelectQuery(sBuilder, selectFields, selectFieldAliases, startMetaCharacter, endMetaCharacter);

		sBuilder.append(" " + SQL.FROM + startMetaCharacter + tableName + endMetaCharacter + " " + SQL.WHERE);
		sBuilder.append(" " + customWhereQuery);

		addOrderByClause(sBuilder, orderBy, orderByTypes, startMetaCharacter, endMetaCharacter);

		return sBuilder.toString();
	}

	public static String buildSelectQuery(String driverName, String tableName, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases,
	        ArrayList<String> orderBy, ArrayList<String> orderByTypes) throws InvalidInputForConfigException, UnimplementedException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildSelectQuery(driverName, tableName, selectFields, selectFieldAliases, orderBy, orderByTypes, startMetaCharacter, endMetaCharacter);
		}
	}

	public static String buildSelectQueryWithWhereColumns(String driverName, String tableName, ArrayList<String> selectColumns,
	        ArrayList<String> selectColumnAliases, ArrayList<String> whereColumns, ArrayList<String> orderBy, ArrayList<String> orderByTypes,
	        boolean keyFieldsCaseSensitive) throws InvalidInputForConfigException, UnimplementedException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildSelectQueryWithWhereColumns(driverName, tableName, selectColumns, selectColumnAliases, whereColumns, orderBy, orderByTypes,
			        keyFieldsCaseSensitive, startMetaCharacter, endMetaCharacter);
		}
	}

	private static String buildSelectQuery(String driverName, String tableName, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases,
	        ArrayList<String> orderBy, ArrayList<String> orderByTypes, char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.SELECT);

		buildSelectQuery(sBuilder, selectFields, selectFieldAliases, startMetaCharacter, endMetaCharacter);

		sBuilder.append(" " + SQL.FROM + startMetaCharacter + tableName + endMetaCharacter);

		addOrderByClause(sBuilder, orderBy, orderByTypes, startMetaCharacter, endMetaCharacter);

		return sBuilder.toString();
	}

	private static String buildSelectQueryWithWhereColumns(String driverName, String tableName, ArrayList<String> selectFields,
	        ArrayList<String> selectFieldAliases, ArrayList<String> whereColumns, ArrayList<String> orderBy, ArrayList<String> orderByTypes,
	        boolean keyFieldsCaseSensitive, char startMetaCharacter, char endMetaCharacter) {

		StringBuilder sBuilder = new StringBuilder(SQL.SELECT);

		buildSelectQuery(sBuilder, selectFields, selectFieldAliases, startMetaCharacter, endMetaCharacter);

		sBuilder.append(" " + SQL.FROM + startMetaCharacter + tableName + endMetaCharacter);

		if (whereColumns != null && !whereColumns.isEmpty()) {
			sBuilder.append(" " + SQL.WHERE + " ");
			addWhereFields(sBuilder, whereColumns, keyFieldsCaseSensitive, startMetaCharacter, endMetaCharacter);
		}

		addOrderByClause(sBuilder, orderBy, orderByTypes, startMetaCharacter, endMetaCharacter);

		return sBuilder.toString();
	}

	private static void addOrderByClause(StringBuilder sBuilder, ArrayList<String> orderBy, ArrayList<String> orderByTypes, char startMetaCharacter,
	        char endMetaCharacter) {
		if (orderBy != null) {
			sBuilder.append(" ORDER BY ");
			for (int i = 0; i < orderBy.size(); i++) {
				sBuilder.append(startMetaCharacter + orderBy.get(i) + endMetaCharacter + " "
				        + (orderByTypes == null || orderByTypes.get(i) == null || orderByTypes.get(i) != null ? "ASC" : orderByTypes.get(i)));

				if (i < orderBy.size() - 1) {
					sBuilder.append(SQL.COMMA);
				}
			}
		}
	}

	private static void addWhereFields(StringBuilder sBuilder, ArrayList<String> whereColumns, boolean keyFieldsCaseSensitive, char startMetaCharacter,
	        char endMetaCharacter) {
		for (int i = 0; i < whereColumns.size(); i++) {
			if (keyFieldsCaseSensitive) {
				sBuilder.append(" BINARY ");
			}
			sBuilder.append(startMetaCharacter + whereColumns.get(i) + endMetaCharacter + " " + SQL.EQUAL_TO + " " + SQL.BINDING_PARAM);
			if (i < whereColumns.size() - 1) {
				sBuilder.append(SQL.AND);
			}
		}
	}

	public static void closeResultSetObject(ResultSet rs) throws SQLException {
		if (rs != null && !rs.isClosed()) {
			rs.close();
		}
	}

	public static void closePreparedStatementObject(PreparedStatement preparedStatement) throws SQLException {
		if (preparedStatement != null && !preparedStatement.isClosed()) {
			preparedStatement.close();
		}
	}

	public static void closeConnectionObjects(Connection connection) throws SQLException {
		SQLConnectionUtil.closeConnectionObjects(connection);
	}

	public static void closeConnectionObjects(Connection connection, PreparedStatement preparedStatement, ResultSet rs) throws SQLException {
		closeResultSetObject(rs);
		closePreparedStatementObject(preparedStatement);
		closeConnectionObjects(connection);
	}

	public static String buildUpdateQuery(String driverName, ArrayList<String> keys, String tableName, ArrayList<String> keyColumns,
	        ArrayList<String> constantFields, ArrayList<String> constantValues, boolean keyFieldsCaseSensitive)
	        throws InvalidInputForConfigException, UnimplementedException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildUpdateQuery(keys, tableName, keyColumns, constantFields, constantValues, keyFieldsCaseSensitive, startMetaCharacter, endMetaCharacter);
		}
	}

	private static String buildUpdateQuery(ArrayList<String> keys, String tableName, ArrayList<String> keyColumns, ArrayList<String> constantFields,
	        ArrayList<String> constantValues, boolean keyFieldsCaseSensitive, char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.UPDATE + startMetaCharacter + tableName + endMetaCharacter + " " + SQL.SET);

		addUpdateColumns(sBuilder, keys, constantFields, constantValues, startMetaCharacter, endMetaCharacter);

		if (keyColumns != null && !keyColumns.isEmpty()) {
			sBuilder.append(" " + SQL.WHERE + " ");
			addWhereFields(sBuilder, keyColumns, keyFieldsCaseSensitive, startMetaCharacter, endMetaCharacter);
		}

		return sBuilder.toString();
	}

	private static void addUpdateColumns(StringBuilder sBuilder, ArrayList<String> keys, ArrayList<String> constantFields, ArrayList<String> constantValues,
	        char startMetaCharacter, char endMetaCharacter) {
		for (int i = 0; i < keys.size(); i++) {
			sBuilder.append(startMetaCharacter + keys.get(i) + endMetaCharacter + " " + General.Operator.EQUAL_TO + " " + SQL.BINDING_PARAM);
			if (i < keys.size() - 1) {
				sBuilder.append(SQL.COMMA);
			}
		}

		if (constantFields != null && constantValues != null) {
			addConstants(sBuilder, constantFields, constantValues, startMetaCharacter, endMetaCharacter);
		}
	}

	private static void addConstants(StringBuilder sBuilder, ArrayList<String> constantFields, ArrayList<String> constantValues, char startMetaCharacter,
	        char endMetaCharacter) {
		sBuilder.append(SQL.COMMA);

		for (int i = 0; i < constantFields.size(); i++) {
			sBuilder.append(startMetaCharacter + constantFields.get(i) + endMetaCharacter + " " + General.Operator.EQUAL_TO + " " + constantValues.get(i));
			if (i < constantFields.size() - 1) {
				sBuilder.append(SQL.COMMA);
			}
		}
	}

	public static String buildInsertQuery(String driverName, ArrayList<String> insertKeys, String tableName)
	        throws UnimplementedException, InvalidInputForConfigException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildInsertQuery(insertKeys, tableName, startMetaCharacter, endMetaCharacter);
		}
	}

	private static String buildInsertQuery(ArrayList<String> insertKeys, String tableName, char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.INSERT + SQL.INTO + startMetaCharacter + tableName + endMetaCharacter + General.OPEN_PARENTHESIS);

		addInsertColumns(sBuilder, insertKeys, startMetaCharacter, endMetaCharacter);
		sBuilder.append(General.CLOSE_PARENTHESIS);

		sBuilder.append(" " + SQL.VALUES + General.OPEN_PARENTHESIS);

		QueryInfo upsertQueryInfo = new QueryInfo();
		addInsertFields(upsertQueryInfo, sBuilder, insertKeys);
		sBuilder.append(General.CLOSE_PARENTHESIS);

		return sBuilder.toString();
	}

	private static void addInsertColumns(StringBuilder sBuilder, ArrayList<String> insertKeys, char startMetaCharacter, char endMetaCharacter) {
		for (int i = 0; i < insertKeys.size(); i++) {
			sBuilder.append(startMetaCharacter + insertKeys.get(i) + endMetaCharacter);
			if (i < insertKeys.size() - 1) {
				sBuilder.append(SQL.COMMA);
			}
		}
	}

	private static void addInsertFields(QueryInfo upsertQueryInfo, StringBuilder sBuilder, ArrayList<String> insertKeys) {
		for (int i = 0; i < insertKeys.size(); i++) {
			upsertQueryInfo.addFieldIndex(insertKeys.get(i));
			sBuilder.append(SQL.BINDING_PARAM);
			if (i < insertKeys.size() - 1) {
				sBuilder.append(SQL.COMMA);
			}
		}
	}

	public static String buildInsertQueryWithConstantFields(String driverName, ArrayList<String> insertKeys, String tableName, ArrayList<String> constantFields,
	        ArrayList<String> constantValues) throws InvalidInputForConfigException, UnimplementedException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildInsertQueryWithConstantFields(driverName, insertKeys, tableName, constantFields, constantValues, startMetaCharacter, endMetaCharacter);
		}
	}

	public static QueryInfo buildInsertIfNotExistQueryWithConstantFields(String driverName, ArrayList<String> insertKeys, String tableName,
	        ArrayList<String> insertConstantFields, ArrayList<String> insertConstantValues) throws InvalidInputForConfigException, UnimplementedException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildInsertIfNotExistQueryWithConstantFields(insertKeys, tableName, insertConstantFields, insertConstantValues, startMetaCharacter,
			        endMetaCharacter);
		}
	}

	private static QueryInfo buildInsertIfNotExistQueryWithConstantFields(ArrayList<String> insertKeys, String tableName, ArrayList<String> constantFields,
	        ArrayList<String> constantValues, char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.INSERT + SQL.INTO + startMetaCharacter + tableName + endMetaCharacter + General.OPEN_PARENTHESIS);

		addColumns(sBuilder, insertKeys, constantFields, startMetaCharacter, endMetaCharacter);

		sBuilder.append(" " + SQL.VALUES + General.OPEN_PARENTHESIS);

		QueryInfo upsertQueryInfo = new QueryInfo();
		addFields(upsertQueryInfo, sBuilder, insertKeys, constantFields, constantValues);

		addOnDuplicateUpdate(sBuilder, insertKeys, startMetaCharacter, endMetaCharacter);

		upsertQueryInfo.setQuery(sBuilder.toString());

		return upsertQueryInfo;
	}

	public static QueryInfo buildUpsertQueryWithConstantFields(String driverName, ArrayList<String> insertKeys, String tableName,
	        ArrayList<String> insertConstantFields, ArrayList<String> insertConstantValues, ArrayList<String> updateConstantFields,
	        ArrayList<String> updateConstantValues, ArrayList<String> metaDataFields, String checksumField, boolean alwaysUpdate)
	        throws InvalidInputForConfigException, UnimplementedException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildUpsertQueryWithConstantFields(insertKeys, tableName, insertConstantFields, insertConstantValues, updateConstantFields,
			        updateConstantValues, metaDataFields, checksumField, alwaysUpdate, startMetaCharacter, endMetaCharacter);
		}
	}

	private static String buildInsertQueryWithConstantFields(String driverName, ArrayList<String> insertKeys, String tableName,
	        ArrayList<String> constantFields, ArrayList<String> constantValues, char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.INSERT + SQL.INTO + startMetaCharacter + tableName + endMetaCharacter + General.OPEN_PARENTHESIS);

		addColumns(sBuilder, insertKeys, constantFields, startMetaCharacter, endMetaCharacter);

		sBuilder.append(" " + SQL.VALUES + General.OPEN_PARENTHESIS);

		QueryInfo upsertQueryInfo = new QueryInfo();
		addFields(upsertQueryInfo, sBuilder, insertKeys, constantFields, constantValues);

		return sBuilder.toString();
	}

	private static QueryInfo buildUpsertQueryWithConstantFields(ArrayList<String> insertKeys, String tableName, ArrayList<String> insertConstantFields,
	        ArrayList<String> insertConstantValues, ArrayList<String> updateConstantFields, ArrayList<String> updateConstantValues,
	        ArrayList<String> metaDataFields, String checksumField, boolean alwaysUpdate, char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.INSERT + SQL.INTO + startMetaCharacter + tableName + endMetaCharacter + General.OPEN_PARENTHESIS);

		addColumns(sBuilder, insertKeys, insertConstantFields, startMetaCharacter, endMetaCharacter);

		sBuilder.append(" " + SQL.VALUES + General.OPEN_PARENTHESIS);

		QueryInfo upsertQueryInfo = new QueryInfo();
		addFields(upsertQueryInfo, sBuilder, insertKeys, insertConstantFields, insertConstantValues);

		if (alwaysUpdate) {
			addOnDuplicateUpdate(upsertQueryInfo, sBuilder, insertKeys, updateConstantFields, updateConstantValues, startMetaCharacter, endMetaCharacter);
		} else {
			ArrayList<String> conditionAndQueryParameterFields = getConditionAndQueryParameterFields(insertKeys, metaDataFields, checksumField,
			        startMetaCharacter, endMetaCharacter);
			addOnDuplicateUpdate(upsertQueryInfo, conditionAndQueryParameterFields, sBuilder, insertKeys, updateConstantFields, updateConstantValues,
			        startMetaCharacter, endMetaCharacter);
		}

		upsertQueryInfo.setQuery(sBuilder.toString());

		return upsertQueryInfo;
	}

	private static ArrayList<String> getConditionAndQueryParameterFields(ArrayList<String> insertKeys, ArrayList<String> metaDataFields, String checksumField,
	        char startMetaCharacter, char endMetaCharacter) {
		ArrayList<String> conditionAndQueryParameterFields = new ArrayList<>();
		if (!StringUtils.isEmpty(checksumField)) {
			conditionAndQueryParameterFields.add(checksumField);
			conditionAndQueryParameterFields.add(SQL.NOT + startMetaCharacter + checksumField + endMetaCharacter + " <=> " + SQL.BINDING_PARAM + ")");
			return conditionAndQueryParameterFields;
		} else if (metaDataFields != null && !metaDataFields.isEmpty()) {
			return getConditionAndQueryParameterFields(insertKeys, metaDataFields, startMetaCharacter, endMetaCharacter);
		} else {
			return getConditionAndQueryParameterFields(insertKeys, startMetaCharacter, endMetaCharacter);
		}
	}

	private static ArrayList<String> getConditionAndQueryParameterFields(ArrayList<String> insertKeys, ArrayList<String> metaDataFields,
	        char startMetaCharacter, char endMetaCharacter) {
		ArrayList<String> conditionAndQueryParameterFields = new ArrayList<>();

		StringBuilder condition = new StringBuilder();
		for (String insertKey : insertKeys) {
			if (!metaDataFields.contains(insertKey)) {
				if (condition.length() != 0) {
					condition.append(" OR ");
				}

				condition.append(SQL.NOT + startMetaCharacter + insertKey + endMetaCharacter + " <=> " + SQL.BINDING_PARAM + ")");
				conditionAndQueryParameterFields.add(insertKey);
			}
		}

		conditionAndQueryParameterFields.add(condition.toString());
		return conditionAndQueryParameterFields;
	}

	private static ArrayList<String> getConditionAndQueryParameterFields(ArrayList<String> insertKeys, char startMetaCharacter, char endMetaCharacter) {
		ArrayList<String> conditionAndQueryParameterFields = new ArrayList<>();

		StringBuilder condition = new StringBuilder();
		for (String insertKey : insertKeys) {
			if (condition.length() != 0) {
				condition.append(" OR ");
			}

			condition.append(SQL.NOT + startMetaCharacter + insertKey + endMetaCharacter + " <=> " + SQL.BINDING_PARAM + ")");
			conditionAndQueryParameterFields.add(insertKey);
		}

		conditionAndQueryParameterFields.add(condition.toString());
		return conditionAndQueryParameterFields;
	}

	private static void addOnDuplicateUpdate(StringBuilder sBuilder, ArrayList<String> insertKeys, char startMetaCharacter, char endMetaCharacter) {
		sBuilder.append(" " + SQL.ON_DUPLICATE_UPDATE + " ");
		sBuilder.append(startMetaCharacter + insertKeys.get(0) + endMetaCharacter + " = " + insertKeys.get(0));
	}

	private static void addOnDuplicateUpdate(QueryInfo upsertQueryInfo, StringBuilder sBuilder, ArrayList<String> insertKeys,
	        ArrayList<String> updateConstantFields, ArrayList<String> updateConstantValues, char startMetaCharacter, char endMetaCharacter) {
		sBuilder.append(" " + SQL.ON_DUPLICATE_UPDATE + " ");
		for (int i = 0; i < insertKeys.size(); i++) {
			if (i != 0) {
				sBuilder.append(SQL.COMMA);
			}

			sBuilder.append(startMetaCharacter + insertKeys.get(i) + endMetaCharacter + " = " + SQL.BINDING_PARAM);
			upsertQueryInfo.addFieldIndex(insertKeys.get(i));
		}

		if (updateConstantFields != null && !updateConstantFields.isEmpty()) {
			for (int i = 0; i < updateConstantFields.size(); i++) {
				sBuilder.append(SQL.COMMA + startMetaCharacter + updateConstantFields.get(i) + endMetaCharacter + " = " + updateConstantValues.get(i));
			}
		}
	}

	private static void addOnDuplicateUpdate(QueryInfo upsertQueryInfo, ArrayList<String> conditionAndQueryParameterFields, StringBuilder sBuilder,
	        ArrayList<String> insertKeys, ArrayList<String> updateConstantFields, ArrayList<String> updateConstantValues, char startMetaCharacter,
	        char endMetaCharacter) {
		sBuilder.append(" " + SQL.ON_DUPLICATE_UPDATE + " ");
		String condition = conditionAndQueryParameterFields.get(conditionAndQueryParameterFields.size() - 1);

		String var = "@elt_core_upsert_var";

		sBuilder.append(startMetaCharacter).append(insertKeys.get(0)).append(endMetaCharacter).append(" = IF(").append(var).append(":=").append(condition)
		        .append(", ").append(SQL.BINDING_PARAM).append(", ").append(startMetaCharacter).append(insertKeys.get(0)).append(endMetaCharacter).append(")");

		addQueryParameterFieldIndexes(upsertQueryInfo, conditionAndQueryParameterFields);
		upsertQueryInfo.addFieldIndex(insertKeys.get(0));

		for (int i = 1; i < insertKeys.size(); i++) {
			sBuilder.append(SQL.COMMA).append(startMetaCharacter).append(insertKeys.get(i)).append(endMetaCharacter).append(" = IF(").append(var).append(", ")
			        .append(SQL.BINDING_PARAM).append(", ").append(startMetaCharacter).append(insertKeys.get(i)).append(endMetaCharacter).append(")");
			upsertQueryInfo.addFieldIndex(insertKeys.get(i));
		}

		if (updateConstantFields != null && !updateConstantFields.isEmpty()) {
			for (int i = 0; i < updateConstantFields.size(); i++) {
				sBuilder.append(SQL.COMMA + startMetaCharacter + updateConstantFields.get(i) + endMetaCharacter + " = IF(" + var + ", "
				        + updateConstantValues.get(i) + ", " + startMetaCharacter + updateConstantFields.get(i) + endMetaCharacter + ")");
			}
		}
	}

	private static void addQueryParameterFieldIndexes(QueryInfo upsertQueryInfo, ArrayList<String> conditionAndQueryParameterFields) {
		for (int i = 0; i < conditionAndQueryParameterFields.size() - 1; i++) {
			upsertQueryInfo.addFieldIndex(conditionAndQueryParameterFields.get(i));
		}
	}

	private static void addColumns(StringBuilder sBuilder, ArrayList<String> insertKeys, ArrayList<String> constantFields, char startMetaCharacter,
	        char endMetaCharacter) {
		addInsertColumns(sBuilder, insertKeys, startMetaCharacter, endMetaCharacter);

		if (constantFields != null) {
			addConstantFields(sBuilder, constantFields, startMetaCharacter, endMetaCharacter);
		}

		sBuilder.append(General.CLOSE_PARENTHESIS);
	}

	private static void addFields(QueryInfo upsertQueryInfo, StringBuilder sBuilder, ArrayList<String> insertKeys, ArrayList<String> constantFields,
	        ArrayList<String> constantValues) {
		addInsertFields(upsertQueryInfo, sBuilder, insertKeys);

		if (constantFields != null && constantValues != null) {
			addConstantValues(sBuilder, constantValues);
		}

		sBuilder.append(General.CLOSE_PARENTHESIS);
	}

	private static void addConstantFields(StringBuilder sBuilder, ArrayList<String> constantFields, char startMetaCharacter, char endMetaCharacter) {
		sBuilder.append(SQL.COMMA);

		for (int i = 0; i < constantFields.size(); i++) {
			sBuilder.append(startMetaCharacter + constantFields.get(i) + endMetaCharacter);
			if (i < constantFields.size() - 1) {
				sBuilder.append(SQL.COMMA);
			}
		}
	}

	private static void addConstantValues(StringBuilder sBuilder, ArrayList<String> constants) {
		sBuilder.append(SQL.COMMA);

		for (int i = 0; i < constants.size(); i++) {
			sBuilder.append(constants.get(i));
			if (i < constants.size() - 1) {
				sBuilder.append(SQL.COMMA);
			}
		}
	}

	public static String buildDropTableQuery(String tableName, String driverName)
	        throws UnsupportedException, InvalidInputForConfigException, UnimplementedException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			StringBuilder stringBuilder = new StringBuilder("DROP TABLE " + startMetaCharacter + tableName + endMetaCharacter + " ");

			return stringBuilder.toString();
		}
	}

	public static String truncateTableQuery(String tableName, String driverName)
	        throws UnsupportedException, UnimplementedException, InvalidInputForConfigException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			StringBuilder stringBuilder = new StringBuilder("TRUNCATE TABLE " + startMetaCharacter + tableName + endMetaCharacter + " ");

			return stringBuilder.toString();
		}
	}

	public static String buildCreateTableQuery(String tableName, String driverName, ArrayList<String> keyFields,
	        LinkedHashMap<String, AnvizentDataType> structure, ArrayList<String> constantFields, ArrayList<String> constantTypes)
	        throws UnsupportedException, UnimplementedException, InvalidInputForConfigException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			return createTable(tableName, keyFields, structure, constantFields, constantTypes, storeType);
		}
	}

	private static String createTable(String tableName, ArrayList<String> keyFields, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> constantFields, ArrayList<String> constantTypes, StoreType storeType) throws UnsupportedException {
		char startMetaCharacter = storeType.getStartMetaCharacter();
		char endMetaCharacter = storeType.getEndMetaCharacter();

		StringBuilder stringBuilder = new StringBuilder("CREATE TABLE");

		stringBuilder.append(" " + startMetaCharacter + tableName + endMetaCharacter + " (\n\t");

		addColumsMetaData(stringBuilder, structure, storeType, constantFields, constantTypes, startMetaCharacter, endMetaCharacter);

		if (keyFields != null) {
			stringBuilder.append(",\n\t");
			addPrimaryKeyConstraint(stringBuilder, keyFields, tableName, startMetaCharacter, endMetaCharacter);
		}

		stringBuilder.append("\n)");

		return stringBuilder.toString();
	}

	private static void addColumsMetaData(StringBuilder stringBuilder, LinkedHashMap<String, AnvizentDataType> structure, StoreType storeType,
	        ArrayList<String> constantFields, ArrayList<String> constantTypes, char startMetaCharacter, char endMetaCharacter) throws UnsupportedException {

		ArrayList<Entry<String, AnvizentDataType>> metaData = new ArrayList<>(structure.entrySet());

		for (int i = 0; i < metaData.size(); i++) {
			Entry<String, AnvizentDataType> entry = metaData.get(i);
			String typeAndSize = StoreType.FACTORIES.get(storeType).getDataType(entry.getValue());

			if (entry.getKey().equals(ELTConstants.ERROR_FULL_DETAILS)) {
				// Only for time being, For Error Handlers
				stringBuilder.append(startMetaCharacter + entry.getKey() + endMetaCharacter + " " + "LONGTEXT");
			} else {
				stringBuilder.append(startMetaCharacter + entry.getKey() + endMetaCharacter + " " + typeAndSize);
			}

			if (i < metaData.size() - 1) {
				stringBuilder.append(SQL.COMMA + "\n\t");
			}
		}

		addConstantFields(stringBuilder, constantFields, constantTypes, startMetaCharacter, endMetaCharacter);
	}

	private static void addConstantFields(StringBuilder stringBuilder, ArrayList<String> constantFields, ArrayList<String> constantTypes,
	        char startMetaCharacter, char endMetaCharacter) {
		if (constantFields != null && !constantFields.isEmpty()) {
			stringBuilder.append(SQL.COMMA);

			for (int i = 0; i < constantFields.size(); i++) {
				stringBuilder.append(startMetaCharacter + constantFields.get(i) + endMetaCharacter + " " + constantTypes.get(i));

				if (i < constantFields.size() - 1) {
					stringBuilder.append(SQL.COMMA + "\n\t");
				}
			}
		}
	}

	private static void addPrimaryKeyConstraint(StringBuilder stringBuilder, ArrayList<String> keyFields, String tableName, char startMetaCharacter,
	        char endMetaCharacter) {
		stringBuilder.append("CONSTRAINT " + startMetaCharacter + tableName + "_pk" + endMetaCharacter + " PRIMARY KEY(");

		for (int i = 0; i < keyFields.size(); i++) {
			stringBuilder.append(keyFields.get(i));

			if (i < keyFields.size() - 1) {
				stringBuilder.append(",");
			}
		}

		stringBuilder.append(")");
	}

	public static String buildSimpleSelectQuery(String tableName, ArrayList<String> keyColumns) {
		StringBuilder stringBuilder = new StringBuilder("SELECT 1 FROM `" + tableName + "` WHERE ");

		for (int i = 0; i < keyColumns.size(); i++) {
			stringBuilder.append('`' + keyColumns.get(i) + '`' + " " + SQL.EQUAL_TO + " ?");

			if (i < keyColumns.size() - 1) {
				stringBuilder.append(" AND ");
			}
		}

		stringBuilder.append("\n LIMIT 1");

		return stringBuilder.toString();
	}

	public static String buildBulkSelectQueryWithWhereColumns(String driverName, String tableName, ArrayList<String> selectFields,
	        ArrayList<String> selectFieldAliases, ArrayList<String> whereColumns, ArrayList<String> orderBy, ArrayList<String> orderByTypes,
	        boolean keyFieldsCaseSensitive, Integer limit) throws UnimplementedException, InvalidInputForConfigException {
		if (driverName == null || driverName.isEmpty()) {
			throw new InvalidInputForConfigException("Driver name cannot be null or empty!");
		} else {
			StoreType storeType = StoreType.getInstance(driverName);
			if (storeType == null) {
				throw new UnimplementedException("Driver '" + driverName + "' is either invalid or not implemented yet!");
			}

			char startMetaCharacter = storeType.getStartMetaCharacter();
			char endMetaCharacter = storeType.getEndMetaCharacter();

			return buildBulkSelectQueryWithWhereColumns(tableName, selectFields, selectFieldAliases, whereColumns, orderBy, orderByTypes,
			        keyFieldsCaseSensitive, limit, startMetaCharacter, endMetaCharacter);
		}
	}

	private static String buildBulkSelectQueryWithWhereColumns(String tableName, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases,
	        ArrayList<String> whereColumns, ArrayList<String> orderBy, ArrayList<String> orderByTypes, boolean keyFieldsCaseSensitive, Integer limit,
	        char startMetaCharacter, char endMetaCharacter) {
		StringBuilder sBuilder = new StringBuilder(SQL.SELECT + " t1.* " + SQL.FROM + " ( " + SQL.SELECT);

		buildSelectQuery(sBuilder, selectFields, selectFieldAliases, startMetaCharacter, endMetaCharacter);
		sBuilder.append(" " + SQL.FROM + startMetaCharacter + tableName + endMetaCharacter);
		if (whereColumns != null && !whereColumns.isEmpty()) {
			sBuilder.append(" " + SQL.WHERE + " ");
			addWhereFields(sBuilder, whereColumns, keyFieldsCaseSensitive, startMetaCharacter, endMetaCharacter);
		}
		addOrderByClause(sBuilder, orderBy, orderByTypes, startMetaCharacter, endMetaCharacter);
		sBuilder.append(" ) AS t1");

		sBuilder.append("\nUNION ALL\n");

		sBuilder.append(SQL.SELECT);
		buildSelectQuery(sBuilder, selectFields, selectFieldAliases, startMetaCharacter, endMetaCharacter);

		sBuilder.append(" " + SQL.FROM + startMetaCharacter + tableName + endMetaCharacter);

		if (whereColumns != null && !whereColumns.isEmpty()) {
			sBuilder.append(" " + SQL.WHERE + " NOT (");
			addWhereFields(sBuilder, whereColumns, keyFieldsCaseSensitive, startMetaCharacter, endMetaCharacter);
			sBuilder.append(" )");
			sBuilder.append(" LIMIT " + limit);// TODO
		}

		return sBuilder.toString();
	}

	public static void addPreparedStatementBatch(PreparedStatement preparedStatement, HashMap<String, Object> row, ArrayList<String> rowKeysAndFieldsIn,
	        String exclude) throws SQLException {
		ArrayList<String> rowKeysAndFields = new ArrayList<>(rowKeysAndFieldsIn);
		rowKeysAndFields.remove(exclude);
		setPreparedStatement(preparedStatement, row, rowKeysAndFields);
		preparedStatement.addBatch();
	}

	public static void executeQuey(String query, SQLConnectionByPartition sqlConnectionByPartition) throws SQLException {
		executeQuey(query, sqlConnectionByPartition.getConnection(TaskContext.getPartitionId()));
	}

	public static void executeQuey(String query, Connection connection) throws SQLException {
		if (StringUtils.isNotEmpty(query)) {
			Statement statement = connection.createStatement();
			statement.execute(query);
			statement.close();
		}
	}
}
