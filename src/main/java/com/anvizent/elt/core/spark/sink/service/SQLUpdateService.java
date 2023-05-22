package com.anvizent.elt.core.spark.sink.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLUpdateService {

	public static void update(PreparedStatement preparedStatement) throws SQLException {
		int update = preparedStatement.executeUpdate();
		if (update <= 0) {
			throw new SQLException(Constants.ExceptionMessage.UNABLE_TO_UPDATE);
		}
	}

	public static ArrayList<String> getDifferFieldsAndKeys(ArrayList<String> keys, ArrayList<String> keyFields, ArrayList<String> keyColumns,
	        ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields) {
		ArrayList<String> differKeysAndFields = getDifferKeys(keys, keyFields, keyColumns);
		differKeysAndFields = getDifferFields(differKeysAndFields, fieldsDifferToColumns, columnsDifferToFields);

		return differKeysAndFields;
	}

	public static ArrayList<String> getDifferKeys(ArrayList<String> keys, ArrayList<String> keyFields, ArrayList<String> keyColumns) {
		ArrayList<String> differKeys = new ArrayList<>(keys);

		if (keyFields != null) {
			for (int i = 0; i < keyFields.size(); i++) {
				String key = keyColumns == null ? keyFields.get(i) : keyColumns.get(i);
				int index = differKeys.indexOf(keyFields.get(i));
				if (index != -1) {
					differKeys.set(index, key);
				}
			}
		}

		return differKeys;
	}

	public static ArrayList<String> getDifferFields(ArrayList<String> keys, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields) {
		ArrayList<String> differFields = new ArrayList<>(keys);

		if (columnsDifferToFields != null && fieldsDifferToColumns != null) {
			for (int i = 0; i < columnsDifferToFields.size(); i++) {
				int index = differFields.indexOf(fieldsDifferToColumns.get(i));
				if (index != -1) {
					differFields.set(index, columnsDifferToFields.get(i));
				}
			}
		}

		return differFields;
	}

	public static ArrayList<String> getColumnsDifferToFields(ArrayList<String> keys, ArrayList<String> keyFields, ArrayList<String> fieldsDifferToColumns,
	        ArrayList<String> columnsDifferToFields) {
		if (keyFields != null) {
			keys.removeAll(keyFields);
		}

		return getDifferFields(keys, fieldsDifferToColumns, columnsDifferToFields);
	}

	public static boolean checkForUpdate(ResultSet resultSet, HashMap<String, Object> row, boolean alwaysUpdate, String checksumField,
	        ArrayList<String> metaDataFields, ArrayList<String> rowKeys, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields)
	        throws SQLException, RecordProcessingException, InvalidConfigValueException {
		if (alwaysUpdate) {
			return true;
		} else if (checksumField != null && !checksumField.isEmpty()) {
			Object rowValue = row.get(checksumField);
			Object resultSetValue = resultSet.getObject(1);

			return checkRowAndResultSetValue(rowValue, resultSetValue);
		} else {
			return checkForUpdate(resultSet, row, metaDataFields, rowKeys, fieldsDifferToColumns, columnsDifferToFields);
		}
	}

	private static boolean checkRowAndResultSetValue(Object rowValue, Object resultSetValue)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if ((rowValue != null && resultSetValue == null) || (rowValue == null && resultSetValue != null)) {
			return true;
		}

		if (rowValue == null && resultSetValue == null) {
			return false;
		}

		if (rowValue.getClass().equals(Date.class)) {
			if (resultSetValue.getClass().equals(java.sql.Date.class)) {
				return ((Date) rowValue).getTime() != ((java.sql.Date) resultSetValue).getTime();
			} else if (resultSetValue.getClass().equals(java.sql.Time.class)) {
				return ((Date) rowValue).getTime() != ((java.sql.Time) resultSetValue).getTime();
			} else if (resultSetValue.getClass().equals(java.sql.Timestamp.class)) {
				return ((Date) rowValue).getTime() != ((java.sql.Timestamp) resultSetValue).getTime();
			} else {
				return true;
			}
		} else if (rowValue.getClass().equals(resultSetValue.getClass())) {
			return !rowValue.equals(resultSetValue);
		} else {
			resultSetValue = TypeConversionUtil.dataTypeConversion(resultSetValue, resultSetValue.getClass(), rowValue.getClass(), null, null, null, null);
			return !rowValue.equals(resultSetValue);
		}
	}

	private static boolean checkForUpdate(ResultSet resultSet, HashMap<String, Object> row, ArrayList<String> metaDataFields, ArrayList<String> rowKeys,
	        ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields)
	        throws SQLException, RecordProcessingException, InvalidConfigValueException {
		try {
			for (int i = 0; i < rowKeys.size(); i++) {
				String rowKey = rowKeys.get(i);
				String columnKey = (columnKey = getDifferColumnKey(rowKey, fieldsDifferToColumns, columnsDifferToFields)) == null ? rowKey : columnKey;

				if (metaDataFields != null && metaDataFields.contains(rowKey)) {
					continue;
				}

				Object rowValue = row.get(rowKey);
				Object resultSetValue = resultSet.getObject(columnKey);

				if (checkRowAndResultSetValue(rowValue, resultSetValue)) {
					return true;
				}
			}

			return false;
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private static String getDifferColumnKey(String rowKey, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields) {
		if (fieldsDifferToColumns != null && columnsDifferToFields != null) {
			int index = fieldsDifferToColumns.indexOf(rowKey);
			if (index != -1) {
				return columnsDifferToFields.get(index);
			}
		}

		return null;
	}

	public static boolean checkForUpdate(HashMap<String, Object> row, ArrayList<Object> values, SQLSinkConfigBean sqlSinkConfigBean)
	        throws InvalidSituationException {
		if (sqlSinkConfigBean.isAlwaysUpdate()) {
			return true;
		} else {
			int i = 0;

			for (String field : sqlSinkConfigBean.getSelectFields()) {
				if (notEquals(row.get(field), values.get(i++))) {
					return true;
				}
			}

			return false;
		}
	}

	public static boolean checkForUpdate(HashMap<String, Object> row, ArrayList<Object> values, boolean alwaysUpdate, ArrayList<String> selectFields)
	        throws InvalidSituationException {
		if (alwaysUpdate) {
			return true;
		} else {
			int i = 0;

			for (String field : selectFields) {
				if (notEquals(row.get(field), values.get(i++))) {
					return true;
				}
			}

			return false;
		}
	}

	public static boolean notEquals(Object obj1, Object obj2) {
		if (obj1 == null && obj2 == null) {
			return false;
		} else {
			return (obj1 == null && obj2 != null) || (obj1 != null && obj2 == null) || (!obj1.equals(obj2));
		}
	}
}
