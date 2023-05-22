package com.anvizent.elt.core.spark.sink.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnection;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnectionByTaskId;
import com.anvizent.elt.core.listener.common.connection.factory.ArangoDBConnectionFactory;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.Constants.NOSQL;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.util.bean.ArangoDBSinkGetResult;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.anvizent.query.builder.ArrangoDBQueryBuilder;
import com.anvizent.query.builder.QueryBuilder;
import com.anvizent.query.builder.exception.InvalidInputException;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.anvizent.universal.query.json.SourceType;
import com.anvizent.universal.query.json.UniversalQueryJson;
import com.anvizent.universal.query.json.v2.Condition;
import com.anvizent.universal.query.json.v2.ConditionType;
import com.anvizent.universal.query.json.v2.WhereClause;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.entity.CollectionEntity;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBSinkService {

	public static boolean checkIfTableExists(ArangoDBSinkConfigBean arangoDBSinkConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
				        .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0];
				Collection<CollectionEntity> collections = arangoDBConnection.db(arangoDBSinkConfigBean.getConnection().getDBName()).getCollections();
				for (CollectionEntity collectionEntity : collections) {
					if (collectionEntity.getName().equals(arangoDBSinkConfigBean.getTableName())) {
						return true;
					}
				}

				return false;
			} catch (Exception e) {
				if (arangoDBSinkConfigBean.getRetryDelay() != null && arangoDBSinkConfigBean.getRetryDelay() > 0) {
					exception = e;
					Thread.sleep(arangoDBSinkConfigBean.getRetryDelay());
				}
			}
		} while (++i < arangoDBSinkConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}

		return false;
	}

	public static void createTable(ArangoDBSinkConfigBean arangoDBSinkConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
				        .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0];
				arangoDBConnection.db(arangoDBSinkConfigBean.getConnection().getDBName()).createCollection(arangoDBSinkConfigBean.getTableName());

				return;
			} catch (Exception e) {
				if (arangoDBSinkConfigBean.getRetryDelay() != null && arangoDBSinkConfigBean.getRetryDelay() > 0) {
					exception = e;
					Thread.sleep(arangoDBSinkConfigBean.getRetryDelay());
				}
			}
		} while (++i < arangoDBSinkConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}
	}

	public static void dropTable(ArangoDBSinkConfigBean arangoDBSinkConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
				        .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0];
				arangoDBConnection.db(arangoDBSinkConfigBean.getConnection().getDBName()).collection(arangoDBSinkConfigBean.getTableName()).drop();

				return;
			} catch (Exception e) {
				if (arangoDBSinkConfigBean.getRetryDelay() != null && arangoDBSinkConfigBean.getRetryDelay() > 0) {
					exception = e;
					Thread.sleep(arangoDBSinkConfigBean.getRetryDelay());
				}
			}
		} while (++i < arangoDBSinkConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}
	}

	public static void truncate(ArangoDBSinkConfigBean arangoDBSinkConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
				        .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0];
				arangoDBConnection.db(arangoDBSinkConfigBean.getConnection().getDBName()).collection(arangoDBSinkConfigBean.getTableName()).truncate();

				return;
			} catch (Exception e) {
				if (arangoDBSinkConfigBean.getRetryDelay() != null && arangoDBSinkConfigBean.getRetryDelay() > 0) {
					exception = e;
					Thread.sleep(arangoDBSinkConfigBean.getRetryDelay());
				}
			}
		} while (++i < arangoDBSinkConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}
	}

	public static LinkedHashMap<String, AnvizentDataType> getNewStructure(ArangoDBSinkConfigBean arangoDBSinkConfigBean,
	        LinkedHashMap<String, AnvizentDataType> oldStructure) throws UnsupportedException {

		LinkedHashMap<String, AnvizentDataType> keyFieldsChangedStructure = changeFieldsToDifferColumns(arangoDBSinkConfigBean.getKeyFields(),
		        arangoDBSinkConfigBean.getKeyColumns(), oldStructure);

		LinkedHashMap<String, AnvizentDataType> fieldsDifferToColumnsChangedStructure = changeFieldsToDifferColumns(
		        arangoDBSinkConfigBean.getFieldsDifferToColumns(), arangoDBSinkConfigBean.getColumnsDifferToFields(), keyFieldsChangedStructure);

		if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			LinkedHashMap<String, AnvizentDataType> structure = addConstants(arangoDBSinkConfigBean, arangoDBSinkConfigBean.getInsertConstantsConfigBean(),
			        fieldsDifferToColumnsChangedStructure);

			return addConstants(arangoDBSinkConfigBean, arangoDBSinkConfigBean.getUpdateConstantsConfigBean(), structure);
		} else {
			return addConstants(arangoDBSinkConfigBean, arangoDBSinkConfigBean.getConstantsConfigBean(), fieldsDifferToColumnsChangedStructure);
		}
	}

	private static void addConstants(LinkedHashMap<String, AnvizentDataType> structure, ArrayList<String> fields, ArrayList<Class<?>> types)
	        throws UnsupportedException {
		for (int i = 0; i < fields.size(); i++) {
			structure.put(fields.get(i), new AnvizentDataType(types.get(i)));
		}
	}

	private static LinkedHashMap<String, AnvizentDataType> addConstants(ArangoDBSinkConfigBean arangoDBSinkConfigBean,
	        NoSQLConstantsConfigBean constantsConfigBean, LinkedHashMap<String, AnvizentDataType> oldStructure) throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> structure = null;

		if (constantsConfigBean.getFields() != null && !constantsConfigBean.getFields().isEmpty()) {
			addConstants(oldStructure, constantsConfigBean.getFields(), constantsConfigBean.getTypes());
		}

		if (constantsConfigBean.getLiteralFields() != null && !constantsConfigBean.getLiteralFields().isEmpty()) {
			addConstants(structure == null ? oldStructure : structure, constantsConfigBean.getLiteralFields(), constantsConfigBean.getLiteralTypes());
		}

		return structure == null ? oldStructure : structure;
	}

	private static LinkedHashMap<String, AnvizentDataType> changeFieldsToDifferColumns(ArrayList<String> fields, ArrayList<String> columns,
	        LinkedHashMap<String, AnvizentDataType> structure) {

		LinkedHashMap<String, AnvizentDataType> keyFieldsChangedStructure;

		if (fields != null && !fields.isEmpty() && columns != null && !columns.isEmpty()) {
			keyFieldsChangedStructure = new LinkedHashMap<>();

			for (Entry<String, AnvizentDataType> entry : structure.entrySet()) {
				String key = entry.getKey();
				int index = fields.indexOf(key);
				if (index != -1) {
					keyFieldsChangedStructure.put(columns.get(index), entry.getValue());
				} else {
					keyFieldsChangedStructure.put(key, entry.getValue());
				}
			}

			return keyFieldsChangedStructure;
		} else {
			return keyFieldsChangedStructure = new LinkedHashMap<>(structure);
		}
	}

	public static void setSelectFields(ArangoDBSinkConfigBean arangoDBSinkConfigBean, Component component) {
		ArrayList<String> rowFields = new ArrayList<>(component.getStructure().keySet());
		if (arangoDBSinkConfigBean.getKeyFields() != null) {
			rowFields.removeAll(arangoDBSinkConfigBean.getKeyFields());
		}
		arangoDBSinkConfigBean.setRowFields(rowFields);

		setSelectFields(arangoDBSinkConfigBean, new ArrayList<>(component.getStructure().keySet()));
	}

	private static void setSelectFields(ArangoDBSinkConfigBean arangoDBSinkConfigBean, ArrayList<String> rowKeys) {
		ArrayList<String> newSelectKeys = SQLUpdateService.getColumnsDifferToFields(rowKeys, arangoDBSinkConfigBean.getKeyFields(),
		        arangoDBSinkConfigBean.getFieldsDifferToColumns(), arangoDBSinkConfigBean.getColumnsDifferToFields());

		arangoDBSinkConfigBean.setSelectFields(newSelectKeys);
	}

	@SuppressWarnings("unused")
	private static void setSelectFieldsWithId(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, ArrayList<String> rowKeys) {
		ArrayList<String> selectFieldsWithId = SQLUpdateService.getColumnsDifferToFields(rowKeys, rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getFieldsDifferToColumns(), rethinkDBSinkConfigBean.getColumnsDifferToFields());
		if (!rowKeys.contains("_key")) {
			selectFieldsWithId.add("_key");
		}

		rethinkDBSinkConfigBean.setSelectFieldsWithId(selectFieldsWithId);
	}

	public static void closeConnection(ArangoDBConnection connection)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		ArangoDB arangoDBConnection = (ArangoDB) ApplicationConnectionBean.getInstance()
		        .get(new ArangoDBConnectionByTaskId(connection, null, TaskContext.getPartitionId()), false)[0];
		if (arangoDBConnection != null) {
			arangoDBConnection.shutdown();
		}
	}

	public static String getOrGenerateId(ArangoDBSinkConfigBean arangoDBSinkConfigBean, HashMap<String, Object> row) throws DataCorruptedException {
		String arangoDBKey = null;
		if (row.containsKey(NOSQL.ARANGO_DB_KEY)) {
			arangoDBKey = row.get(NOSQL.ARANGO_DB_KEY).toString();
		} else if (arangoDBSinkConfigBean.isGenerateId() && arangoDBSinkConfigBean.getKeyFields() != null && !arangoDBSinkConfigBean.getKeyFields().isEmpty()) {
			arangoDBKey = generateId(arangoDBSinkConfigBean.getKeyFields(), row);
		}

		return arangoDBKey;
	}

	private static String generateId(ArrayList<String> keyFields, HashMap<String, Object> row) {
		StringBuilder stringBuilder = new StringBuilder();

		for (int i = 0; i < keyFields.size(); i++) {
			stringBuilder.append("'" + row.get(keyFields.get(i)) + "'");

			if (i < keyFields.size() - 1) {
				stringBuilder.append(',');
			}
		}

		return stringBuilder.toString();
	}

	public static boolean canAvoidSelect(ArangoDBSinkConfigBean arangoDBSinkConfigBean, String arangoDBKey) {
		return (!arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT))
		        && (((!isInsertModeContainsUpdate(arangoDBSinkConfigBean) || CollectionUtil.isEmpty(arangoDBSinkConfigBean.getMetaDataFields()))
		                && arangoDBKey != null)
		                || (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)
		                        && CollectionUtil.isEmpty(arangoDBSinkConfigBean.getKeyFields()))
		                || (arangoDBKey != null && isInsertModeContainsUpdate(arangoDBSinkConfigBean)
		                        && !CollectionUtil.isEmpty(arangoDBSinkConfigBean.getMetaDataFields())
		                        && !arangoDBSinkConfigBean.getBatchType().equals(BatchType.NONE))
		                || (arangoDBKey != null && arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)));
	}

	public static boolean isInsertModeContainsUpdate(ArangoDBSinkConfigBean arangoDBSinkConfigBean) {
		return arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE);
	}

	public static ArangoDBSinkGetResult getRethinkDBSinkGetResult(ArangoDBSinkConfigBean arangoDBSinkConfigBean, HashMap<String, Object> row,
	        String arangoDBKey, boolean canAvoidSelect) throws TimeoutException, ValidationViolationException, RecordProcessingException,
	        ClassNotFoundException, InvalidInputException, UnderConstructionException, ParseException, SQLException, IOException {
		ArangoDBSinkGetResult arangoDBSinkGetResult = new ArangoDBSinkGetResult();
		arangoDBSinkGetResult.setKey(arangoDBKey);

		if (!canAvoidSelect) {
			HashMap<String, Object> result = getResults(arangoDBSinkConfigBean, row, arangoDBKey);
			if (result != null && !result.isEmpty()) {
				arangoDBSinkGetResult.setResult(result);
				arangoDBSinkGetResult.setKey(result.get(NOSQL.ARANGO_DB_KEY).toString());

				if (arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)
				        || arangoDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					try {
						boolean doUpdate = checkForUpdate(row, arangoDBSinkConfigBean.isGenerateId(), result, arangoDBSinkConfigBean.getRowFields(),
						        arangoDBSinkConfigBean.getSelectFields(), arangoDBSinkConfigBean.getMetaDataFields(),
						        arangoDBSinkConfigBean.getFieldsDifferToColumns(), arangoDBSinkConfigBean.getColumnsDifferToFields(),
						        arangoDBSinkConfigBean.getDBInsertMode());

						arangoDBSinkGetResult.setDoUpdate(doUpdate);
					} catch (InvalidConfigValueException exception) {
						throw new ValidationViolationException(exception.getMessage(), exception);
					} catch (UnsupportedException exception) {
						throw new RecordProcessingException(exception.getMessage(), exception);
					}
				}
			}
		}

		return arangoDBSinkGetResult;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static HashMap<String, Object> getResults(ArangoDBSinkConfigBean arangoDBSinkConfigBean, HashMap<String, Object> row, String arangoDBKey)
	        throws TimeoutException, ClassNotFoundException, InvalidInputException, UnderConstructionException, ParseException, ImproperValidationException,
	        UnimplementedException, SQLException, InvalidSituationException, UnsupportedCoerceException, IOException, DateParseException {
		HashMap<String, Object> result;

		ArangoDB arangoDBConnection = ArangoDBConnectionFactory.createConnection(arangoDBSinkConfigBean.getConnection(), TaskContext.getPartitionId());
		if (arangoDBKey == null || arangoDBKey.isEmpty()) {
			UniversalQueryJson universalQueryJson = builduniversalQueryJson(row, arangoDBSinkConfigBean);
			ArrangoDBQueryBuilder arrangoDBQueryBuilder = (ArrangoDBQueryBuilder) QueryBuilder.getQueryBuilder(SourceType.ARANGO_DB);
			Object query = arrangoDBQueryBuilder.build(universalQueryJson);

			ArangoCursor<HashMap<String, Object>> arangoDBCursor = (ArangoCursor<HashMap<String, Object>>) arrangoDBQueryBuilder.execute(query,
			        universalQueryJson,
			        ApplicationConnectionBean.getInstance()
			                .get(new ArangoDBConnectionByTaskId(arangoDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0],
			        HashMap.class);
			if (arangoDBCursor.hasNext()) {
				result = arangoDBCursor.next();
			} else {
				result = null;
			}

			if (arangoDBCursor != null) {
				arangoDBCursor.close();
				arangoDBConnection.shutdown();
			}
		} else {
			ArangoCursor<LinkedHashMap> arangoDBCursor = arangoDBConnection.db(arangoDBSinkConfigBean.getConnection().getDBName())
			        .query("RETURN DOCUMENT(\"" + arangoDBSinkConfigBean.getTableName() + "\", \"" + arangoDBKey + "\")", null, null, LinkedHashMap.class);
			if (arangoDBCursor.hasNext()) {
				result = arangoDBCursor.next();
			} else {
				result = null;
			}

			if (arangoDBCursor != null) {
				arangoDBCursor.close();
				arangoDBConnection.shutdown();
			}
		}

		return result;
	}

	private static UniversalQueryJson builduniversalQueryJson(HashMap<String, Object> row, ArangoDBSinkConfigBean arangoDBSinkConfigBean)
	        throws ClassNotFoundException, InvalidSituationException, UnsupportedCoerceException, DateParseException {
		UniversalQueryJson universalQueryJson = new UniversalQueryJson();
		WhereClause whereClause = new WhereClause();
		ArrayList<WhereClause> andConditions = new ArrayList<>();

		for (int i = 0; i < arangoDBSinkConfigBean.getKeyFields().size(); i++) {
			WhereClause andWhereClause = new WhereClause();

			String field = arangoDBSinkConfigBean.getKeyColumns() == null || arangoDBSinkConfigBean.getKeyColumns().isEmpty()
			        ? arangoDBSinkConfigBean.getKeyFields().get(i)
			        : arangoDBSinkConfigBean.getKeyColumns().get(i);

			Condition andCondition;
			if (row.get(arangoDBSinkConfigBean.getKeyFields().get(i)) == null) {
				andCondition = new Condition(field, ConditionType.EQ.getValue(), null, null, null);
			} else if (row.get(arangoDBSinkConfigBean.getKeyFields().get(i)) != null
			        && row.get(arangoDBSinkConfigBean.getKeyFields().get(i)).getClass().equals(Date.class)) {
				andCondition = new Condition(field, ConditionType.EQ.getValue(),
				        TypeConversionUtil.dateToOtherConversion((Date) row.get(arangoDBSinkConfigBean.getKeyFields().get(i)), Long.class, null).toString(),
				        Long.class.getName(), null);
			} else {
				andCondition = new Condition(field, ConditionType.EQ.getValue(), row.get(arangoDBSinkConfigBean.getKeyFields().get(i)).toString(),
				        row.get(arangoDBSinkConfigBean.getKeyFields().get(i)).getClass().getName(), null);
			}

			andWhereClause.setCondition(andCondition);

			andConditions.add(andWhereClause);
		}

		whereClause.setAnd(andConditions);
		universalQueryJson.setTableName(arangoDBSinkConfigBean.getTableName());
		universalQueryJson.setDbName(arangoDBSinkConfigBean.getConnection().getDBName());
		universalQueryJson.setWhereClauseV2(whereClause);

		return universalQueryJson;
	}

	public static boolean checkForUpdate(HashMap<String, Object> row, boolean generateId, HashMap<String, Object> selectRow, ArrayList<String> rowFields,
	        ArrayList<String> selectFields, ArrayList<String> metaDataFields, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields,
	        DBInsertMode dbInsertMode) throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        InvalidConfigValueException, UnsupportedException {
		if (!dbInsertMode.equals(DBInsertMode.UPSERT) && (generateId || row.containsKey(NOSQL.ARANGO_DB_KEY))
		        && (metaDataFields == null || metaDataFields.isEmpty())) {
			return true;
		} else {
			return checkForUpdate(row, selectRow, rowFields, selectFields, metaDataFields, fieldsDifferToColumns, columnsDifferToFields);
		}
	}

	public static boolean checkForUpdate(HashMap<String, Object> row, HashMap<String, Object> selectRow, ArrayList<String> rowFields,
	        ArrayList<String> selectFields, ArrayList<String> metaDataFields, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException,
	        UnsupportedException {

		for (int i = 0; i < rowFields.size(); i++) {
			if (metaDataFields != null && metaDataFields.contains(rowFields.get(i))) {
				continue;
			}

			Object rowValue = row.get(rowFields.get(i));
			Object resultSetValue = selectRow.get(selectFields.get(i));

			if (rowValue != null && rowValue.getClass().equals(Date.class)) {
				rowValue = ((Date) rowValue).getTime();
			}

			if (resultSetValue != null && resultSetValue.getClass().equals(Long.class)) {
				resultSetValue = (long) resultSetValue;
			}

			rowValue = convertIntoArangoDBType(rowValue, resultSetValue);

			if ((rowValue == null && resultSetValue != null) || (rowValue != null && resultSetValue == null)
			        || (rowValue != null && resultSetValue != null && !resultSetValue.equals(rowValue))) {
				return true;
			}
		}

		return false;
	}

	private static Object convertIntoArangoDBType(Object rowValue, Object resultSetValue)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if (rowValue == null) {
			return null;
		} else if (resultSetValue == null) {
			return null;
		} else {
			return TypeConversionUtil.dataTypeConversion(rowValue, rowValue.getClass(), resultSetValue.getClass(), null, null, null, null);
		}
	}

	public static void ifKeyExistsThenPut(HashMap<String, Object> row, String arangoDBKey) throws DataCorruptedException {
		if (arangoDBKey != null) {
			row.put(NOSQL.ARANGO_DB_KEY, arangoDBKey);
		}
	}

	public static void convertToArangoDBType(HashMap<String, Object> newRow) throws InvalidSituationException, UnsupportedCoerceException, DateParseException {
		for (Entry<String, Object> entry : newRow.entrySet()) {
			if (entry.getValue() != null) {
				if (entry.getValue().getClass().equals(Date.class)) {
					newRow.put(entry.getKey(), TypeConversionUtil.dateToOtherConversion((Date) entry.getValue(), Long.class, null));
				} else {
					continue;
				}
			}
		}
	}

	public static boolean verifyUpdate(HashMap<String, Object> oldDoc, HashMap<String, Object> newDoc, ArrayList<String> metaDataFields,
	        LinkedHashMap<String, AnvizentDataType> newStructure)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		removeArangoSchema(oldDoc, metaDataFields, newStructure);
		removeArangoSchema(newDoc, metaDataFields, newStructure);

		if ((oldDoc != null && !oldDoc.isEmpty()) && (newDoc != null && !newDoc.isEmpty()) && !oldDoc.equals(newDoc)) {
			return true;
		} else {
			return false;
		}
	}

	private static void removeArangoSchema(HashMap<String, Object> arangoDocument, ArrayList<String> metaDataFields,
	        LinkedHashMap<String, AnvizentDataType> newStructure)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if (arangoDocument != null && !arangoDocument.isEmpty()) {
			arangoDocument.remove(NOSQL.ARANGO_DB_ID);
			arangoDocument.remove(NOSQL.ARANGO_DB_KEY);
			arangoDocument.remove(NOSQL.ARANGO_DB_REVISION);

			if (metaDataFields != null && !metaDataFields.isEmpty()) {
				for (String metaDataField : metaDataFields) {
					arangoDocument.remove(metaDataField);
				}
			}

			changeToStructType(arangoDocument, newStructure);
		}
	}

	private static void changeToStructType(HashMap<String, Object> arangoDocument, LinkedHashMap<String, AnvizentDataType> newStructure)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		for (String docKey : arangoDocument.keySet()) {
			if (arangoDocument.get(docKey) != null) {
				arangoDocument.put(docKey, TypeConversionUtil.dataTypeConversion(arangoDocument.get(docKey), arangoDocument.get(docKey).getClass(),
				        newStructure.get(docKey).getJavaType(), null, null, null, null));
			}
		}
	}

	public static String getUpsertQuery(String arangoDBKey, HashMap<String, Object> newRow, String tableName, ArrayList<String> metaDataFields,
	        boolean waitForSync) throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		StringBuilder stringBuilder = new StringBuilder("UPSERT { \"_key\" :  \"" + arangoDBKey + "\" }\n");

		stringBuilder.append("INSERT { ");
		putSubQuery(stringBuilder, newRow, null);
		stringBuilder.append(" }\n");

		stringBuilder.append("UPDATE { ");
		putSubQuery(stringBuilder, newRow, metaDataFields);
		stringBuilder.append(" }\n");

		stringBuilder.append("IN " + tableName + " OPTIONS { ignoreRevs: true " + (waitForSync ? ", waitForSync : " + true : "") + "}\n");

		stringBuilder.append("RETURN { old: OLD, new: NEW }");

		return stringBuilder.toString();
	}

	private static void putSubQuery(StringBuilder stringBuilder, HashMap<String, Object> newRow, ArrayList<String> metaDataFields)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		int i = 0;

		for (String key : newRow.keySet()) {
			if (metaDataFields != null && !metaDataFields.isEmpty()) {
				if (!metaDataFields.contains(key)) {
					stringBuilder.append(getKeyValue(key, newRow.get(key)));

					if (i < newRow.size() - 1) {
						stringBuilder.append(", ");
					}
				}
			} else {
				stringBuilder.append(getKeyValue(key, newRow.get(key)));

				if (i < newRow.size() - 1) {
					stringBuilder.append(", ");
				}
			}
			i++;
		}
	}

	private static String getKeyValue(String key, Object value)
	        throws UnderConstructionException, InvalidSituationException, UnsupportedCoerceException, DateParseException {
		if (value == null) {
			return key + " : " + null;
		} else if (isString(value.getClass().getName())) {
			return key + " : \"" + value + "\"";
		} else if (isNumber(value.getClass().getName())) {
			return key + " : " + value;
		} else if (isDate(value.getClass().getName())) {
			return key + " : " + TypeConversionUtil.dateToOtherConversion((Date) value, Long.class, null);
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

	public static String getInsertQuery(HashMap<String, Object> newRow, String tableName, boolean waitForSync)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		StringBuilder stringBuilder = new StringBuilder("INSERT {\n");

		putSubQuery(stringBuilder, newRow, null);

		stringBuilder.append(" }\n");

		stringBuilder.append("INTO " + tableName + " OPTIONS { keepNull : true " + (waitForSync ? ", waitForSync : " + true : "") + "}\n");

		return stringBuilder.toString();
	}

	public static String getInsertIfNotExistsQuery(String arangoDBKey, ArrayList<String> keyFields, HashMap<String, Object> newRow, String tableName,
	        boolean waitForSync) throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		StringBuilder stringBuilder = new StringBuilder("UPSERT { ");

		if (arangoDBKey != null && !arangoDBKey.isEmpty()) {
			stringBuilder.append("\"_key\" :  \"" + arangoDBKey + "\" }\n");
		} else {
			stringBuilder.append(putKeyFieldsFilter(keyFields, newRow) + " }\n");
		}

		stringBuilder.append("INSERT { ");
		putSubQuery(stringBuilder, newRow, null);
		stringBuilder.append(" }\n");

		stringBuilder.append("UPDATE { }\n ");

		stringBuilder.append("IN " + tableName + " OPTIONS { ignoreRevs: true " + (waitForSync ? ", waitForSync : " + true : "") + "}\n");

		stringBuilder.append("RETURN { old: OLD, new: NEW }");

		return stringBuilder.toString();
	}

	private static String putKeyFieldsFilter(ArrayList<String> keyFields, HashMap<String, Object> newRow)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		String keyFieldsFilter = "";
		int i = 0;
		for (String keyField : keyFields) {
			keyFieldsFilter += getKeyValue(keyField, newRow.get(keyField));

			if (i < keyFields.size() - 1) {
				keyFieldsFilter += ", ";
			}

			i++;
		}

		return keyFieldsFilter;
	}

	public static void skipMetaDataFields(HashMap<String, Object> newRow, ArrayList<String> metaDataFields) {
		if (metaDataFields != null && !metaDataFields.isEmpty()) {
			for (String metaDataField : metaDataFields) {
				newRow.remove(metaDataField);
			}
		}
	}

	public static String getFilterQuery(String tableName, Object[] keyValues) {
		StringBuilder stringBuilder = new StringBuilder("FOR doc IN " + tableName + "\n");

		stringBuilder.append("FILTER doc._key IN [ ");

		for (int i = 0; i < keyValues.length; i++) {
			stringBuilder.append("\"" + keyValues[i] + "\"");

			if (i < keyValues.length - 1) {
				stringBuilder.append(", ");
			}
		}

		stringBuilder.append(" ]\n");
		stringBuilder.append("RETURN doc");

		return stringBuilder.toString();
	}

	public static HashMap<String, Object> getRecord(ArangoDBSinkConfigBean arangoDBSinkConfigBean, HashMap<String, Object> row, String arangoDBKey,
	        NoSQLConstantsConfigBean constants, ArrayList<String> metaDataFields, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ArrayList<ExpressionEvaluator> expressionEvaluators) throws UnsupportedCoerceException, InvalidSituationException, DateParseException,
	        ImproperValidationException, DataCorruptedException, ValidationViolationException, InvalidConfigValueException {
		HashMap<String, Object> differKeysAndFieldsRow = RowUtil.changeFieldsToDifferColumns(row, arangoDBSinkConfigBean.getKeyFields(),
		        arangoDBSinkConfigBean.getKeyColumns(), arangoDBSinkConfigBean.getFieldsDifferToColumns(), arangoDBSinkConfigBean.getColumnsDifferToFields());

		HashMap<String, Object> newRow = RowUtil.addElements(differKeysAndFieldsRow, NoSQLConstantsService.getConstants(null,
		        arangoDBSinkConfigBean.getEmptyRow(), arangoDBSinkConfigBean.getEmptyArguments(), null, constants, expressionEvaluators), newStructure);

		convertToArangoDBType(newRow);
		ifKeyExistsThenPut(newRow, arangoDBKey);
		skipMetaDataFields(newRow, metaDataFields);

		return newRow;
	}
}
