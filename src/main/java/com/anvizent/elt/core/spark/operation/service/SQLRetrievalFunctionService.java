package com.anvizent.elt.core.spark.operation.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.collections.CollectionUtils;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.SQLFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;
import com.anvizent.elt.core.spark.util.CollectionUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLRetrievalFunctionService {

	public static HashMap<String, Object> getLookUpRow(SQLLookUpConfigBean sqlLookUpConfigBean, PreparedStatement selectPreparedStatement,
	        LinkedHashMap<String, Object> whereKeyValues) throws SQLException, InvalidLookUpException {

		HashMap<String, Object> newObject = new HashMap<>();
		HashMap<String, Object> newRow = new HashMap<>();

		setPreparedStatement(selectPreparedStatement, whereKeyValues.values(), 1);

		System.out.println(selectPreparedStatement);

		ResultSet resultSet = selectPreparedStatement.executeQuery();

		int rowsCount = 0;

		if (resultSet.next()) {
			++rowsCount;
			putSelectedValues(newRow, resultSet,
			        sqlLookUpConfigBean.getSelectFieldAliases() == null || sqlLookUpConfigBean.getSelectFieldAliases().isEmpty()
			                ? sqlLookUpConfigBean.getSelectColumns()
			                : sqlLookUpConfigBean.getSelectFieldAliases());

			if (resultSet.next() && sqlLookUpConfigBean.isLimitTo1()) {
				SQLUtil.closeResultSetObject(resultSet);
				throw new InvalidLookUpException(
				        MessageFormat.format(ExceptionMessage.MORE_THAN_ONE_ROW_IN_LOOKUP_TABLE, sqlLookUpConfigBean.getTableName(), whereKeyValues));
			}
		}

		SQLUtil.closeResultSetObject(resultSet);

		newObject.put(General.LOOKUP_CACHED, false);
		newObject.put(General.LOOKUP_RESULTING_ROW, newRow);
		newObject.put(General.LOOKEDUP_ROWS_COUNT, rowsCount);

		return newObject;
	}

	private static void putSelectedValues(HashMap<String, Object> newRow, ResultSet resultSet, ArrayList<String> selectFields) throws SQLException {
		for (int i = 0; i < selectFields.size(); i++) {
			newRow.put(selectFields.get(i), TypeConversionUtil.convertToGeneralType(resultSet.getObject(i + 1)));
		}
	}

	public static HashMap<String, Object> checkInsertOnZeroFetch(SQLRetrievalConfigBean sqlRetrievalConfigBean, PreparedStatement insertPreparedStatement,
	        ArrayList<Object> whereValues, HashMap<String, Object> row, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure)
	        throws ClassNotFoundException, SQLException, ValidationViolationException, InvalidConfigValueException, DateParseException,
	        UnsupportedCoerceException, ImproperValidationException, InvalidSituationException, InvalidLookUpException {
		HashMap<String, Object> newRow = new HashMap<>();

		if (sqlRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			insertOnZeroFetch(sqlRetrievalConfigBean, insertPreparedStatement, whereValues, newRow, row, structure, newStructure);
		} else if (sqlRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.FAIL)) {
			throw new InvalidLookUpException(ExceptionMessage.FAILED_ON_ZERO_FETCH);
		} else {
			return newRow;
		}

		return newRow;
	}

	private static void insertOnZeroFetch(SQLRetrievalConfigBean sqlRetrievalConfigBean, PreparedStatement insertPreparedStatement,
	        ArrayList<Object> whereValues, HashMap<String, Object> newRow, HashMap<String, Object> row, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure) throws SQLException, ClassNotFoundException, InvalidConfigValueException, DateParseException,
	        UnsupportedCoerceException, ImproperValidationException, InvalidSituationException {

		setInsertValues(sqlRetrievalConfigBean, insertPreparedStatement, newRow, row, structure, newStructure);
		setWhereValues(sqlRetrievalConfigBean, insertPreparedStatement, whereValues);

		ArrayList<String> selectFields;
		if (CollectionUtils.isNotEmpty(sqlRetrievalConfigBean.getSelectFieldAliases())) {
			selectFields = sqlRetrievalConfigBean.getSelectFieldAliases();
		} else {
			selectFields = sqlRetrievalConfigBean.getSelectColumns();
		}

		insert(insertPreparedStatement, selectFields, sqlRetrievalConfigBean.getAiColumnIndex(), newRow);
	}

	private static void setInsertValues(SQLRetrievalConfigBean sqlRetrievalConfigBean, PreparedStatement insertPreparedStatement,
	        HashMap<String, Object> newRow, HashMap<String, Object> row, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure) throws ClassNotFoundException, SQLException, DateParseException, UnsupportedCoerceException,
	        ImproperValidationException, InvalidSituationException, InvalidConfigValueException {
		boolean hasInsertByFields = CollectionUtil.isAnyNotEmpty(sqlRetrievalConfigBean.getInsertValueByFields());
		boolean hasNoDateFormats = CollectionUtil.isAllEmpty(sqlRetrievalConfigBean.getInsertValuesByFieldFormats());

		for (int i = 0, paramIndex = 1; i < sqlRetrievalConfigBean.getInsertValues().size(); i++, paramIndex++) {
			if (isAIColumn(sqlRetrievalConfigBean, i)) {
				paramIndex--;
				continue;
			}

			String field;
			if (CollectionUtils.isNotEmpty(sqlRetrievalConfigBean.getSelectFieldAliases())) {
				field = sqlRetrievalConfigBean.getSelectFieldAliases().get(i);
			} else {
				field = sqlRetrievalConfigBean.getSelectColumns().get(i);
			}

			Object obj;
			if (isInsertByField(sqlRetrievalConfigBean, i, hasInsertByFields)) {
				obj = getValueFromField(sqlRetrievalConfigBean.getInsertValueByFields().get(i), field,
				        hasNoDateFormats ? null : sqlRetrievalConfigBean.getInsertValuesByFieldFormats().get(i), row, structure, newStructure);
			} else {
				obj = sqlRetrievalConfigBean.getInsertConstantValues().get(field);
			}

			insertPreparedStatement.setObject(paramIndex, obj);
			if (CollectionUtils.isNotEmpty(sqlRetrievalConfigBean.getSelectFieldAliases())) {
				newRow.put(sqlRetrievalConfigBean.getSelectFieldAliases().get(i), obj);
			} else {
				newRow.put(sqlRetrievalConfigBean.getSelectColumns().get(i), obj);
			}
		}
	}

	private static Object getValueFromField(String field, String selectField, String dateFormat, HashMap<String, Object> row,
	        LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		return TypeConversionUtil.dataTypeConversion(row.get(field), structure.get(field).getJavaType(), newStructure.get(selectField).getJavaType(),
		        dateFormat, null, Operation.General.INSERT_VALUE_BY_FIELDS, null);
	}

	private static boolean isAIColumn(SQLRetrievalConfigBean sqlRetrievalConfigBean, int i) {
		return sqlRetrievalConfigBean.isUseAIValue() && sqlRetrievalConfigBean.getAiColumnIndex() != null && sqlRetrievalConfigBean.getAiColumnIndex() == i;
	}

	private static boolean isInsertByField(SQLRetrievalConfigBean sqlRetrievalConfigBean, int i, boolean hasInsertByFields) {
		return hasInsertByFields && sqlRetrievalConfigBean.getInsertValueByFields().get(i) != null
		        && !sqlRetrievalConfigBean.getInsertValueByFields().get(i).isEmpty();
	}

	private static void setWhereValues(SQLRetrievalConfigBean sqlRetrievalConfigBean, PreparedStatement insertPreparedStatement, ArrayList<Object> whereValues)
	        throws SQLException {
		int index = sqlRetrievalConfigBean.getInsertValues().size() + 1;
		if (sqlRetrievalConfigBean.isUseAIValue() && sqlRetrievalConfigBean.getAiColumnIndex() != null && sqlRetrievalConfigBean.getAiColumnIndex() >= 0) {
			index--;
		}
		setPreparedStatement(insertPreparedStatement, whereValues, index);
	}

	private static void insert(PreparedStatement insertPreparedStatement, ArrayList<String> selectFields, Integer aiColumnIndex, HashMap<String, Object> newRow)
	        throws SQLException {
		String query = insertPreparedStatement.toString();
		int update = insertPreparedStatement.executeUpdate();
		if (update <= 0) {
			throw new SQLException("Unable to insert.");
		} else if (aiColumnIndex != null && aiColumnIndex >= 0) {
			ResultSet resultSet = insertPreparedStatement.getGeneratedKeys();

			if (resultSet != null && resultSet.next()) {
				newRow.put(selectFields.get(aiColumnIndex), resultSet.getObject(1));
			} else {
				System.out.println(SQLRetrievalFunctionService.class + " => getGeneratedKeys is empty for query => " + query);
			}
		}
	}

	private static void setPreparedStatement(PreparedStatement preparedStatement, Collection<Object> whereValues, int index) throws SQLException {
		for (Object whereValue : whereValues) {
			preparedStatement.setObject(index++, whereValue);
		}
	}

	public static LinkedHashMap<String, Object> getWhereKeyValues(SQLRetrievalConfigBean sqlRetrievalConfigBean, HashMap<String, Object> row) {
		LinkedHashMap<String, Object> whereKeyValues = new LinkedHashMap<>();

		for (int i = 0; i < sqlRetrievalConfigBean.getWhereFields().size(); i++) {
			String key = sqlRetrievalConfigBean.getWhereColumns() == null || sqlRetrievalConfigBean.getWhereColumns().isEmpty()
			        ? sqlRetrievalConfigBean.getWhereFields().get(i)
			        : sqlRetrievalConfigBean.getWhereColumns().get(i);
			whereKeyValues.put(key, row.get(sqlRetrievalConfigBean.getWhereFields().get(i)));
		}

		return whereKeyValues;
	}

	public static ArrayList<Object> getWhereValues(SQLRetrievalConfigBean sqlRetrievalConfigBean, HashMap<String, Object> row) {
		return new ArrayList<>(getWhereKeyValues(sqlRetrievalConfigBean, row).values());
	}

	public static HashMap<String, Object> getFetcherRows(SQLFetcherConfigBean sqlFetcherConfigBean, PreparedStatement selectPreparedStatement,
	        LinkedHashMap<String, Object> whereKeyValues) throws SQLException, InvalidLookUpException {

		HashMap<String, Object> newObject = new HashMap<>();
		ArrayList<HashMap<String, Object>> newRows = new ArrayList<>();

		setPreparedStatement(selectPreparedStatement, whereKeyValues.values(), 1);

		System.out.println("execute query started ( " + new Date() + " ) => table => " + sqlFetcherConfigBean.getTableName() + "(single select)");
		ResultSet resultSet = selectPreparedStatement.executeQuery();
		System.out.println("execute query completed ( " + new Date() + " ) => table => " + sqlFetcherConfigBean.getTableName() + "(single select)");

		int rowsCount = 0;

		while (resultSet.next() && ++rowsCount <= sqlFetcherConfigBean.getMaxFetchLimit()) {
			HashMap<String, Object> newRow = new HashMap<>();

			for (String columnName : sqlFetcherConfigBean.getSelectFieldAliases() == null || sqlFetcherConfigBean.getSelectFieldAliases().isEmpty()
			        ? sqlFetcherConfigBean.getSelectColumns()
			        : sqlFetcherConfigBean.getSelectFieldAliases()) {
				newRow.put(columnName, TypeConversionUtil.convertToGeneralType(resultSet.getObject(columnName)));
			}

			newRows.add(newRow);
		}

		if (resultSet.next()) {
			throw new InvalidLookUpException(MessageFormat.format(ExceptionMessage.MORE_THAN_EXPECTED_ROWS_IN_FETCHER_TABLE,
			        sqlFetcherConfigBean.getMaxFetchLimit(), sqlFetcherConfigBean.getTableName(), whereKeyValues));
		}

		SQLUtil.closeResultSetObject(resultSet);

		newObject.put(General.LOOKUP_CACHED, false);
		newObject.put(General.LOOKUP_RESULTING_ROW, newRows);
		newObject.put(General.LOOKEDUP_ROWS_COUNT, rowsCount);

		return newObject;
	}

	public static void setBulkSelectPreparedStatement(PreparedStatement bulkSelectPreparedStatement, HashMap<String, Object> whereKeyValues)
	        throws SQLException {
		setPreparedStatement(bulkSelectPreparedStatement, whereKeyValues.values(), 1);
		setPreparedStatement(bulkSelectPreparedStatement, whereKeyValues.values(), whereKeyValues.size() + 1);
	}

	public static HashMap<String, Object> getCacheKeys(ResultSet resultSet, SQLRetrievalConfigBean sqlRetrievalConfigBean) throws SQLException {
		HashMap<String, Object> columnsAndValues = new HashMap<>();

		ArrayList<String> whereFields;

		if (CollectionUtils.isNotEmpty(sqlRetrievalConfigBean.getWhereColumns())) {
			whereFields = sqlRetrievalConfigBean.getWhereColumns();
		} else {
			whereFields = sqlRetrievalConfigBean.getWhereFields();
		}

		for (String column : whereFields) {
			columnsAndValues.put(column, resultSet.getObject(column));
		}

		return columnsAndValues;
	}

	public static HashMap<String, Object> getRetrievalRowFromResultSet(ResultSet resultSet, SQLRetrievalConfigBean sqlRetrievalConfigBean) throws SQLException {
		HashMap<String, Object> newRow = new HashMap<>();
		putSelectedValues(newRow, resultSet, sqlRetrievalConfigBean.getSelectAndWhereFields());

		return newRow;
	}
}
