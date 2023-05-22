package com.anvizent.elt.core.spark.operation.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ArangoDBLookUp;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBRetrievalFunctionService {

	public static String buildSelectQuery(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, HashMap<String, Object> row)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		if (arangoDBRetrievalConfigBean.getCustomWhere() == null || arangoDBRetrievalConfigBean.getCustomWhere().isEmpty()) {
			return buildSelectQueryWithWhereColumns(row, arangoDBRetrievalConfigBean.getTableName(), arangoDBRetrievalConfigBean.getSelectFields(),
			        arangoDBRetrievalConfigBean.getSelectFieldAliases(), arangoDBRetrievalConfigBean.getWhereFields(),
			        (arangoDBRetrievalConfigBean.getWhereColumns() == null || arangoDBRetrievalConfigBean.getWhereColumns().isEmpty())
			                ? arangoDBRetrievalConfigBean.getWhereFields()
			                : arangoDBRetrievalConfigBean.getWhereColumns(),
			        arangoDBRetrievalConfigBean.getOrderByFields(), arangoDBRetrievalConfigBean.getOrderByType());
		} else {
			return buildSelectQueryWithCustomWhere(arangoDBRetrievalConfigBean.getTableName(), arangoDBRetrievalConfigBean.getSelectFields(),
			        arangoDBRetrievalConfigBean.getSelectFieldAliases(), getCustomWhereQuery(arangoDBRetrievalConfigBean, row),
			        arangoDBRetrievalConfigBean.getOrderByFields(), arangoDBRetrievalConfigBean.getOrderByType());
		}
	}

	public static String buildSelectQueryWithWhereColumns(HashMap<String, Object> row, String tableName, ArrayList<String> selectFields,
	        ArrayList<String> selectFieldAliases, ArrayList<String> whereFields, ArrayList<String> whereColumns, ArrayList<String> orderByFields,
	        String orderByType) throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		StringBuilder query = new StringBuilder("FOR t IN " + tableName);

		buildFilterQuery(query, whereFields, whereColumns, row);
		buildOrderByQuery(query, orderByFields, orderByType);
		buildReturnQuery(query, selectFields, selectFieldAliases == null || selectFieldAliases.isEmpty() ? selectFields : selectFieldAliases);

		return query.toString();
	}

	private static void buildFilterQuery(StringBuilder query, ArrayList<String> whereFields, ArrayList<String> whereColumns, HashMap<String, Object> row)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		query.append("\nFILTER ");
		for (int i = 0; i < whereFields.size(); i++) {
			query.append("t." + whereColumns.get(i) + " == " + getKeyValue(row.get(whereFields.get(i))));

			if (i < whereFields.size() - 1) {
				query.append(" && ");
			}
		}
	}

	private static void buildOrderByQuery(StringBuilder query, ArrayList<String> orderByFields, String orderByType) {
		if (orderByFields != null && !orderByFields.isEmpty()) {
			query.append("\nSORT ");
			for (int i = 0; i < orderByFields.size(); i++) {
				query.append("t." + orderByFields.get(i));

				if (i < orderByFields.size() - 1) {
					query.append(", ");
				}
			}

			if (orderByType != null && !orderByType.isEmpty()) {
				query.append(" " + orderByType);
			}
		}
	}

	private static void buildReturnQuery(StringBuilder query, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases) {
		query.append("\nRETURN {");
		for (int i = 0; i < selectFields.size(); i++) {
			query.append(selectFieldAliases.get(i) + " : t." + selectFields.get(i));

			if (i < selectFields.size() - 1) {
				query.append(", ");
			}
		}
		query.append(" }");
	}

	public static String getCustomWhereQuery(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, HashMap<String, Object> row)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		ArrayList<String> whereFields = new ArrayList<String>();

		String customQuery = replaceWhereFields(arangoDBRetrievalConfigBean.getCustomWhere(), whereFields, row);
		arangoDBRetrievalConfigBean.setWhereFields(whereFields);

		return customQuery;
	}

	private static String replaceWhereFields(String query, ArrayList<String> whereFields, HashMap<String, Object> row)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		String field = StringUtils.substringBetween(query, Constants.General.WHERE_FIELDS_PLACEHOLDER_START, Constants.General.WHERE_FIELDS_PLACEHOLDER_END);
		whereFields.add(field);
		query = StringUtils.replaceOnce(query, Constants.General.WHERE_FIELDS_PLACEHOLDER_START + field + Constants.General.WHERE_FIELDS_PLACEHOLDER_END,
		        field == null || field.isEmpty() ? null : getKeyValue(row.get(field)));

		if (query.contains(Constants.General.WHERE_FIELDS_PLACEHOLDER_START)) {
			query = replaceWhereFields(query, whereFields, row);
		}

		return query;
	}

	public static String buildSelectQueryWithCustomWhere(String tableName, ArrayList<String> selectFields, ArrayList<String> selectFieldAliases,
	        String customWhereQuery, ArrayList<String> orderByFields, String orderByType) {
		StringBuilder query = new StringBuilder("FOR t IN " + tableName);

		buildCustomFilterQuery(query, customWhereQuery);
		buildOrderByQuery(query, orderByFields, orderByType);
		buildReturnQuery(query, selectFields, selectFieldAliases == null || selectFieldAliases.isEmpty() ? selectFields : selectFieldAliases);

		return query.toString();
	}

	private static void buildCustomFilterQuery(StringBuilder query, String customWhereQuery) {
		query.append("\nFILTER " + customWhereQuery);
	}

	public static void buildInsertQuery(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		if (arangoDBRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT) && arangoDBRetrievalConfigBean.getInsertValues() != null
		        && !arangoDBRetrievalConfigBean.getInsertValues().isEmpty()) {
			arangoDBRetrievalConfigBean
			        .setInsertQuery(buildInsertQuery(arangoDBRetrievalConfigBean.getTableName(), arangoDBRetrievalConfigBean.getSelectFields(),
			                arangoDBRetrievalConfigBean.getCollectionInsertValues(), arangoDBRetrievalConfigBean.isWaitForSync()));
		}
	}

	private static String buildInsertQuery(String tableName, ArrayList<String> selectFields, ArrayList<Object> insertValues, boolean waitForSync)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		StringBuilder query = new StringBuilder("INSERT {");

		for (int i = 0; i < selectFields.size(); i++) {
			query.append(selectFields.get(i) + " : " + getKeyValue(insertValues.get(i)));
			if (i < selectFields.size() - 1) {
				query.append(", ");
			}
		}

		query.append(" } IN " + tableName + (waitForSync ? " OPTIONS { waitForSync : " + true + " }" : ""));

		return query.toString();
	}

	private static String getKeyValue(Object value)
	        throws UnderConstructionException, InvalidSituationException, UnsupportedCoerceException, DateParseException {
		if (value == null) {
			return null;
		} else if (isString(value.getClass().getName())) {
			return " \"" + value + "\"";
		} else if (isNumber(value.getClass().getName())) {
			return value + "";
		} else if (isDate(value.getClass().getName())) {
			return TypeConversionUtil.dateToOtherConversion((Date) value, Long.class, null) + "";
		} else {
			throw new UnderConstructionException("Field type '" + value.getClass().getName() + "' is not implemented.");
		}
	}

	private static boolean isDate(String className) {
		return className.equals(Date.class.getName());
	}

	private static boolean isNumber(String className) {
		return className.equals(Byte.class.getName()) || className.equals(Short.class.getName()) || className.equals(Integer.class.getName())
		        || className.equals(Long.class.getName()) || className.equals(Float.class.getName()) || className.equals(Double.class.getName())
		        || className.equals(BigDecimal.class.getName()) || className.equals(Boolean.class.getName());
	}

	private static boolean isString(String className) {
		return className.equals(String.class.getName()) || className.equals(Character.class.getName());
	}

	public static void setInsertValues(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if (arangoDBRetrievalConfigBean.getInsertValues() != null && !arangoDBRetrievalConfigBean.getInsertValues().isEmpty()) {
			ArrayList<Object> outputRowInsertValues = new ArrayList<>();
			ArrayList<Object> collectionInsertValues = new ArrayList<>();

			for (int i = 0; i < arangoDBRetrievalConfigBean.getInsertValues().size(); i++) {
				String dateFormat = (arangoDBRetrievalConfigBean.getSelectFieldDateFormats() == null
				        || arangoDBRetrievalConfigBean.getSelectFieldDateFormats().isEmpty()) ? null
				                : arangoDBRetrievalConfigBean.getSelectFieldDateFormats().get(i);
				Object outputValue = convertToSelectFieldType(arangoDBRetrievalConfigBean.getInsertValues().get(i),
				        arangoDBRetrievalConfigBean.getSelectFieldTypes().get(i).getJavaType(), dateFormat);
				if (arangoDBRetrievalConfigBean.getSelectFieldTypes().get(i).getJavaType().equals(Date.class)) {
					collectionInsertValues.add(TypeConversionUtil.dateTypeConversion(outputValue, Date.class, Long.class, null, null));
				} else {
					collectionInsertValues.add(outputValue);
				}

				outputRowInsertValues.add(outputValue);
			}

			arangoDBRetrievalConfigBean.setCollectionInsertValues(collectionInsertValues);
			arangoDBRetrievalConfigBean.setRowInsertValues(outputRowInsertValues);
		}
	}

	@SuppressWarnings("rawtypes")
	private static Object convertToSelectFieldType(Object fieldValue, Class fieldType, String dateFormat)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if (fieldValue == null) {
			return null;
		} else {
			return TypeConversionUtil.dataTypeConversion(fieldValue, fieldValue.getClass(), fieldType, dateFormat, null,
			        ArangoDBLookUp.SELECT_FIELD_DATE_FORMATS, null);
		}
	}

	public static HashMap<String, Object> getBindValues(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, HashMap<String, Object> row) {
		HashMap<String, Object> bindValues = new HashMap<>();

		putBindValues(bindValues, row, arangoDBRetrievalConfigBean.getWhereFields(),
		        arangoDBRetrievalConfigBean.getWhereColumns() == null || arangoDBRetrievalConfigBean.getWhereColumns().isEmpty()
		                ? arangoDBRetrievalConfigBean.getWhereFields()
		                : arangoDBRetrievalConfigBean.getWhereColumns());
		return bindValues;
	}

	private static void putBindValues(HashMap<String, Object> bindValues, HashMap<String, Object> row, ArrayList<String> whereFields,
	        ArrayList<String> whereColumns) {
		for (int i = 0; i < whereFields.size(); i++) {
			bindValues.put(whereColumns.get(i), row.get(whereFields.get(i)));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static HashMap<String, Object> getLookUpRow(ArangoDBLookUpConfigBean arangoDBLookUpConfigBean, HashMap<String, Object> row)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException, InvalidLookUpException, IOException,
	        UnsupportedCoerceException, InvalidSituationException, DateParseException, InvalidConfigValueException, ArangoDBException,
	        UnderConstructionException {
		HashMap<String, Object> newObject = new HashMap<>();
		HashMap<String, Object> newRow = new HashMap<>();

		int rowsCount = 0;

		ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
		        .get(new ArangoDBConnectionByTaskId(arangoDBLookUpConfigBean.getArangoDBConnection(), null, TaskContext.getPartitionId()), true)[0];
		ArangoCursor<HashMap> arangoCursor = arangoDBConnection.db(arangoDBLookUpConfigBean.getArangoDBConnection().getDBName())
		        .query(buildSelectQuery(arangoDBLookUpConfigBean, row), null, null, HashMap.class);

		if (arangoCursor != null && arangoCursor.hasNext()) {
			++rowsCount;
			newRow.putAll(convertToSelectFieldType(arangoCursor.next(),
			        arangoDBLookUpConfigBean.getSelectFieldAliases() == null || arangoDBLookUpConfigBean.getSelectFieldAliases().isEmpty()
			                ? arangoDBLookUpConfigBean.getSelectFields()
			                : arangoDBLookUpConfigBean.getSelectFieldAliases(),
			        arangoDBLookUpConfigBean.getSelectFieldTypes(), arangoDBLookUpConfigBean.getSelectFieldDateFormats()));

			if (arangoCursor.hasNext() && arangoDBLookUpConfigBean.isLimitTo1()) {
				// TODO show for what row (whereValues as key, value pair)
				throw new InvalidLookUpException(
				        MessageFormat.format(ExceptionMessage.MORE_THAN_ONE_ROW_IN_LOOKUP_TABLE, arangoDBLookUpConfigBean.getTableName()));
			}

			// TODO RnD for effects of cursor close
			// arangoCursor.close();
			// arangoDBConnection.shutdown();
		}

		newObject.put(General.LOOKUP_CACHED, false);
		newObject.put(General.LOOKUP_RESULTING_ROW, newRow);
		newObject.put(General.LOOKEDUP_ROWS_COUNT, rowsCount);

		return newObject;
	}

	private static Map<? extends String, ? extends Object> convertToSelectFieldType(HashMap<String, Object> selectRow, ArrayList<String> selectFields,
	        ArrayList<AnvizentDataType> selectFieldTypes, ArrayList<String> selectFieldDateFormats)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		HashMap<String, Object> newRow = new HashMap<>();

		for (int i = 0; i < selectFields.size(); i++) {
			Object fieldValue = selectRow.get(selectFields.get(i));
			newRow.put(selectFields.get(i), convertToSelectFieldType(fieldValue, selectFieldTypes.get(i).getJavaType(),
			        (selectFieldDateFormats == null || selectFieldDateFormats.isEmpty()) ? null : selectFieldDateFormats.get(i)));
		}

		return newRow;
	}

	public static HashMap<String, Object> checkInsertOnZeroFetch(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean)
	        throws InvalidLookUpException, ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		HashMap<String, Object> newRow = new HashMap<>();

		if (arangoDBRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			insertOnZeroFetch(arangoDBRetrievalConfigBean, newRow);
		} else if (arangoDBRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.FAIL)) {
			throw new InvalidLookUpException(ExceptionMessage.FAILED_ON_ZERO_FETCH);
		} else {
			return newRow;
		}

		return newRow;
	}

	private static void insertOnZeroFetch(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, HashMap<String, Object> newRow)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		putInsertValues(newRow,
		        arangoDBRetrievalConfigBean.getSelectFieldAliases() == null || arangoDBRetrievalConfigBean.getSelectFieldAliases().isEmpty()
		                ? arangoDBRetrievalConfigBean.getSelectFields()
		                : arangoDBRetrievalConfigBean.getSelectFieldAliases(),
		        arangoDBRetrievalConfigBean.getRowInsertValues());
		insert(arangoDBRetrievalConfigBean);
	}

	private static void insert(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
		        .get(new ArangoDBConnectionByTaskId(arangoDBRetrievalConfigBean.getArangoDBConnection(), null, TaskContext.getPartitionId()), true)[0];
		arangoDBConnection.db(arangoDBRetrievalConfigBean.getArangoDBConnection().getDBName()).query(arangoDBRetrievalConfigBean.getInsertQuery(), null, null,
		        null);
	}

	private static void putInsertValues(HashMap<String, Object> newRow, ArrayList<String> selectFields, ArrayList<Object> insertValues) {
		for (int i = 0; i < selectFields.size(); i++) {
			newRow.put(selectFields.get(i), insertValues.get(i));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static HashMap<String, Object> getFetcherRows(ArangoDBFetcherConfigBean arangoDBFetcherConfigBean, HashMap<String, Object> row)
	        throws InvalidLookUpException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        InvalidConfigValueException, UnimplementedException, SQLException, TimeoutException, IOException, ArangoDBException, UnderConstructionException {
		HashMap<String, Object> newObject = new HashMap<>();
		ArrayList<HashMap<String, Object>> newRows = new ArrayList<>();

		int rowsCount = 0;

		ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
		        .get(new ArangoDBConnectionByTaskId(arangoDBFetcherConfigBean.getArangoDBConnection(), null, TaskContext.getPartitionId()), true)[0];
		ArangoCursor<HashMap> arangoCursor = arangoDBConnection.db(arangoDBFetcherConfigBean.getArangoDBConnection().getDBName())
		        .query(buildSelectQuery(arangoDBFetcherConfigBean, row), null, null, HashMap.class);

		while (arangoCursor.hasNext() && ++rowsCount <= arangoDBFetcherConfigBean.getMaxFetchLimit()) {
			HashMap<String, Object> newRow = new HashMap<>();
			newRow.putAll(convertToSelectFieldType(arangoCursor.next(),
			        arangoDBFetcherConfigBean.getSelectFieldAliases() == null || arangoDBFetcherConfigBean.getSelectFieldAliases().isEmpty()
			                ? arangoDBFetcherConfigBean.getSelectFields()
			                : arangoDBFetcherConfigBean.getSelectFieldAliases(),
			        arangoDBFetcherConfigBean.getSelectFieldTypes(), arangoDBFetcherConfigBean.getSelectFieldDateFormats()));
			newRows.add(newRow);
		}

		if (arangoCursor != null) {
			// arangoCursor.close();
			// arangoDBConnection.shutdown();
		}

		if (arangoCursor.hasNext()) {
			throw new InvalidLookUpException(MessageFormat.format(ExceptionMessage.MORE_THAN_EXPECTED_ROWS_IN_FETCHER_TABLE,
			        arangoDBFetcherConfigBean.getMaxFetchLimit(), arangoDBFetcherConfigBean.getTableName()));
		}

		newObject.put(General.LOOKUP_CACHED, false);
		newObject.put(General.LOOKUP_RESULTING_ROW, newRows);
		newObject.put(General.LOOKEDUP_ROWS_COUNT, rowsCount);

		return newObject;
	}

	public static HashMap<String, Object> getCacheBindValues(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, HashMap<String, Object> row)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		if (arangoDBRetrievalConfigBean.getCustomWhere() == null || arangoDBRetrievalConfigBean.getCustomWhere().isEmpty()) {
			return getBindValues(arangoDBRetrievalConfigBean, row);
		} else {
			getCustomWhereQuery(arangoDBRetrievalConfigBean, row);
			return getBindValues(arangoDBRetrievalConfigBean, row);
		}
	}

}
