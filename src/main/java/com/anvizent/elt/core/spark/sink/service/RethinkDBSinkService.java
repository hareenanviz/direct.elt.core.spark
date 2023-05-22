package com.anvizent.elt.core.spark.sink.service;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.types.DataTypes;
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
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnection;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.Constants.NOSQL;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.util.bean.RethinkDBSinkGetResult;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlNonExistenceError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import flexjson.JSONSerializer;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings("rawtypes")
public class RethinkDBSinkService {

	public static void ifRIdExistsThenPut(HashMap<String, Object> row, Object rId) throws DataCorruptedException {
		if (rId != null) {
			row.put(NOSQL.RETHINK_DB_ID, rId);
		}
	}

	public static String getErrorMessage(Map result) {
		return (result.get(NOSQL.RethinkDB.Response.FIRST_ERROR) != null ? result.get(NOSQL.RethinkDB.Response.FIRST_ERROR) + "" : "");
	}

	public static boolean containsIdOrGenerateId(HashMap<String, Object> row, RethinkDBSinkConfigBean rethinkDBSinkConfigBean) throws DataCorruptedException {
		return row.containsKey(NOSQL.RETHINK_DB_ID) || rethinkDBSinkConfigBean.isGenerateId();
	}

	public static Object getOrGenerateId(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, HashMap<String, Object> row) throws DataCorruptedException {
		Object rId = null;
		if (row.containsKey(NOSQL.RETHINK_DB_ID)) {
			rId = row.get(NOSQL.RETHINK_DB_ID);
		} else if (rethinkDBSinkConfigBean.isGenerateId() && rethinkDBSinkConfigBean.getKeyFields() != null
		        && !rethinkDBSinkConfigBean.getKeyFields().isEmpty()) {
			rId = RethinkDBSinkService.generateId(rethinkDBSinkConfigBean.getKeyFields(), row);
		}

		return rId;
	}

	public static boolean canAvoidSelect(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, Object rId) {
		return ((!isInsertModeContainsUpdate(rethinkDBSinkConfigBean) || CollectionUtil.isEmpty(rethinkDBSinkConfigBean.getMetaDataFields())) && rId != null)
		        || (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT) && CollectionUtil.isEmpty(rethinkDBSinkConfigBean.getKeyFields()))
		        || (rId != null && isInsertModeContainsUpdate(rethinkDBSinkConfigBean) && !CollectionUtil.isEmpty(rethinkDBSinkConfigBean.getMetaDataFields())
		                && !rethinkDBSinkConfigBean.getBatchType().equals(BatchType.NONE));
	}

	public static boolean isInsertModeContainsUpdate(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) {
		return rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE) || rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT);
	}

	public static String generateId(ArrayList<String> keyFields, HashMap<String, Object> row) throws DataCorruptedException {
		ArrayList<String> ids = new ArrayList<>();
		for (String key : keyFields) {
			String id = row.get(key) + "";
			if (id.length() > 127) {
				throw new DataCorruptedException("Key length found '" + id.length() + "', which is exceeding max length of '" + 127 + "' for record: " + row);
			}
			ids.add(id);
		}

		return new JSONSerializer().exclude("*.class").include("*.*").serialize(ids);
	}

	public static void convertToRethinkDBType(HashMap<String, Object> newRow, ZoneOffset zoneOffset)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		for (Entry<String, Object> entry : newRow.entrySet()) {
			if (entry.getValue() != null) {
				if (entry.getValue().getClass().equals(Date.class)) {
					newRow.put(entry.getKey(),
					        TypeConversionUtil.dateToOffsetDateTypeConversion(entry.getValue(), Date.class, OffsetDateTime.class, zoneOffset));
				} else if (entry.getValue().getClass().equals(Character.class)) {
					newRow.put(entry.getKey(),
					        TypeConversionUtil.dataTypeConversion(entry.getValue(), Character.class, String.class, null, null, null, zoneOffset));
				} else {
					continue;
				}
			}
		}
	}

	public static boolean checkForUpdate(HashMap<String, Object> row, boolean generateId, Map<String, Object> rSelectRow, ArrayList<String> rowFields,
	        ArrayList<String> selectFields, ArrayList<String> metaDataFields, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields,
	        ZoneOffset timeZoneOffset) throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        InvalidConfigValueException, UnsupportedException {
		if ((generateId || row.containsKey(NOSQL.RETHINK_DB_ID)) && (metaDataFields == null || metaDataFields.isEmpty())) {
			return true;
		} else {
			return checkForUpdate(row, rSelectRow, rowFields, selectFields, metaDataFields, fieldsDifferToColumns, columnsDifferToFields, timeZoneOffset);
		}
	}

	public static boolean checkForUpdate(HashMap<String, Object> row, Map<String, Object> rSelectRow, ArrayList<String> rowFields,
	        ArrayList<String> selectFields, ArrayList<String> metaDataFields, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields,
	        ZoneOffset timeZoneOffset) throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        InvalidConfigValueException, UnsupportedException {

		for (int i = 0; i < rowFields.size(); i++) {
			if (metaDataFields != null && metaDataFields.contains(rowFields.get(i))) {
				continue;
			}

			Object rowValue = row.get(rowFields.get(i));
			Object resultSetValue = rSelectRow.get(selectFields.get(i));

			if (rowValue != null && rowValue.getClass().equals(Date.class)) {
				rowValue = (double) ((Date) rowValue).getTime();
			}

			if (resultSetValue != null && resultSetValue.getClass().equals(OffsetDateTime.class)) {
				Date resultSetDate = (Date) TypeConversionUtil.dataTypeConversion(resultSetValue, OffsetDateTime.class,
				        new AnvizentDataType(DataTypes.DateType).getJavaType(), null, null, null, timeZoneOffset);
				Timestamp timestamp = (Timestamp) TypeConversionUtil.convertFromRethinkDBTypes(resultSetDate);
				resultSetValue = (double) timestamp.getTime();
			}

			rowValue = convertIntoRethinkType(rowValue, resultSetValue, timeZoneOffset);

			if ((rowValue == null && resultSetValue != null) || (rowValue != null && resultSetValue == null)
			        || (rowValue != null && resultSetValue != null && !resultSetValue.equals(rowValue))) {
				return true;
			}
		}

		return false;
	}

	@SuppressWarnings("unused")
	public static boolean checkIfTableExists(RethinkDBSinkConfigBean rethinkDBConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				boolean tableExists = RethinkDB.r.tableList().contains(rethinkDBConfigBean.getTableName()).run((Connection) ApplicationConnectionBean
				        .getInstance().get(new RethinkDBConnectionByTaskId(rethinkDBConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);

				return tableExists;
			} catch (Exception e) {
				exception = e;
				if (rethinkDBConfigBean.getRetryDelay() != null && rethinkDBConfigBean.getRetryDelay() > 0) {
					Thread.sleep(rethinkDBConfigBean.getRetryDelay());
				}
			}
		} while (++i < rethinkDBConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}

		return false;
	}

	public static void dropTable(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				RethinkDB.r.tableDrop(rethinkDBSinkConfigBean.getTableName()).run((Connection) ApplicationConnectionBean.getInstance()
				        .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);

				return;
			} catch (Exception e) {
				exception = e;
				if (rethinkDBSinkConfigBean.getRetryDelay() != null && rethinkDBSinkConfigBean.getRetryDelay() > 0) {
					Thread.sleep(rethinkDBSinkConfigBean.getRetryDelay());
				}
			}
		} while (++i < rethinkDBSinkConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}
	}

	public static void createTable(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				RethinkDB.r.tableCreate(rethinkDBSinkConfigBean.getTableName()).run((Connection) ApplicationConnectionBean.getInstance()
				        .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);

				return;
			} catch (Exception e) {
				exception = e;
				if (rethinkDBSinkConfigBean.getRetryDelay() != null && rethinkDBSinkConfigBean.getRetryDelay() > 0) {
					Thread.sleep(rethinkDBSinkConfigBean.getRetryDelay());
				}
			}
		} while (++i < rethinkDBSinkConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}
	}

	public static void truncate(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) throws Exception {
		int i = 0;
		Exception exception = null;

		do {
			try {
				exception = null;
				RethinkDB.r.table(rethinkDBSinkConfigBean.getTableName()).delete().run((Connection) ApplicationConnectionBean.getInstance()
				        .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);

				return;
			} catch (Exception e) {
				exception = e;
				if (rethinkDBSinkConfigBean.getRetryDelay() != null && rethinkDBSinkConfigBean.getRetryDelay() > 0) {
					Thread.sleep(rethinkDBSinkConfigBean.getRetryDelay());
				}
			}
		} while (++i < rethinkDBSinkConfigBean.getMaxRetryCount());

		if (exception != null) {
			throw exception;
		}
	}

	public static void closeConnection(RethinkDBConnection rethinkDBConnection)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		Connection connection = (Connection) ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkDBConnection, null, TaskContext.getPartitionId()), false)[0];
		if (connection != null) {
			connection.close();
		}
	}

	public static void setSelectFields(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, Component component) {
		ArrayList<String> rowFields = new ArrayList<>(component.getStructure().keySet());
		if (rethinkDBSinkConfigBean.getKeyFields() != null) {
			rowFields.removeAll(rethinkDBSinkConfigBean.getKeyFields());
		}
		rethinkDBSinkConfigBean.setRowFields(rowFields);

		setSelectFields(rethinkDBSinkConfigBean, new ArrayList<>(component.getStructure().keySet()));
		setSelectFieldsWithId(rethinkDBSinkConfigBean, new ArrayList<>(component.getStructure().keySet()));
	}

	private static void setSelectFields(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, ArrayList<String> rowKeys) {
		ArrayList<String> newSelectKeys = SQLUpdateService.getColumnsDifferToFields(rowKeys, rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getFieldsDifferToColumns(), rethinkDBSinkConfigBean.getColumnsDifferToFields());

		rethinkDBSinkConfigBean.setSelectFields(newSelectKeys);
	}

	private static void setSelectFieldsWithId(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, ArrayList<String> rowKeys) {
		ArrayList<String> selectFieldsWithId = SQLUpdateService.getColumnsDifferToFields(rowKeys, rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getFieldsDifferToColumns(), rethinkDBSinkConfigBean.getColumnsDifferToFields());
		if (!rowKeys.contains("id")) {
			selectFieldsWithId.add("id");
		}

		rethinkDBSinkConfigBean.setSelectFieldsWithId(selectFieldsWithId);
	}

	public static void setTimeZoneDetails(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) {
		ZoneOffset timeZoneOffset = OffsetDateTime.now().getOffset();
		rethinkDBSinkConfigBean.setTimeZoneOffset(timeZoneOffset);
	}

	public static LinkedHashMap<String, AnvizentDataType> getNewStructure(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, Component component)
	        throws UnsupportedException {

		LinkedHashMap<String, AnvizentDataType> keyFieldsChangedStructure = changeFieldsToDifferColumns(rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getKeyColumns(), component.getStructure());

		LinkedHashMap<String, AnvizentDataType> fieldsDifferToColumnsChangedStructure = changeFieldsToDifferColumns(
		        rethinkDBSinkConfigBean.getFieldsDifferToColumns(), rethinkDBSinkConfigBean.getColumnsDifferToFields(), keyFieldsChangedStructure);

		if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			LinkedHashMap<String, AnvizentDataType> structure = addConstants(rethinkDBSinkConfigBean, rethinkDBSinkConfigBean.getInsertConstantsConfigBean(),
			        fieldsDifferToColumnsChangedStructure);

			return addConstants(rethinkDBSinkConfigBean, rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(), structure);
		} else {
			return addConstants(rethinkDBSinkConfigBean, rethinkDBSinkConfigBean.getConstantsConfigBean(), fieldsDifferToColumnsChangedStructure);
		}
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

	private static LinkedHashMap<String, AnvizentDataType> addConstants(RethinkDBSinkConfigBean rethinkDBSinkConfigBean,
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

	private static void addConstants(LinkedHashMap<String, AnvizentDataType> structure, ArrayList<String> fields, ArrayList<Class<?>> types)
	        throws UnsupportedException {
		for (int i = 0; i < fields.size(); i++) {
			structure.put(fields.get(i), new AnvizentDataType(types.get(i)));
		}
	}

	private static Object convertIntoRethinkType(Object rowValue, Object resultSetValue, ZoneOffset timeZoneOffset)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if (rowValue == null) {
			return null;
		} else if (resultSetValue == null) {
			return null;
		} else {
			return TypeConversionUtil.dataTypeConversion(rowValue, rowValue.getClass(), resultSetValue.getClass(), null, null, null, timeZoneOffset);
		}
	}

	public static RethinkDBSinkGetResult getRethinkDBSinkGetResult(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, HashMap<String, Object> row, Table table,
	        Object rId, boolean canAvoidSelect)
	        throws DataCorruptedException, TimeoutException, SQLException, ValidationViolationException, RecordProcessingException {
		RethinkDBSinkGetResult rethinkDBSinkGetResult = new RethinkDBSinkGetResult();
		rethinkDBSinkGetResult.setrId(rId);

		if (!canAvoidSelect) {
			Cursor<? extends Map<String, Object>> resultSet = getResultSet(rethinkDBSinkConfigBean, row, rId, table);
			if (resultSet != null && resultSet.hasNext()) {
				Map<String, Object> result = resultSet.next();
				rethinkDBSinkGetResult.setResult(result);
				rId = result.remove(NOSQL.RETHINK_DB_ID);

				if (rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)
				        || rethinkDBSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					try {
						boolean doUpdate = RethinkDBSinkService.checkForUpdate(row, rethinkDBSinkConfigBean.isGenerateId(), result,
						        rethinkDBSinkConfigBean.getRowFields(), rethinkDBSinkConfigBean.getSelectFields(), rethinkDBSinkConfigBean.getMetaDataFields(),
						        rethinkDBSinkConfigBean.getFieldsDifferToColumns(), rethinkDBSinkConfigBean.getColumnsDifferToFields(),
						        rethinkDBSinkConfigBean.getTimeZoneOffset());
						rethinkDBSinkGetResult.setDoUpdate(doUpdate);
					} catch (InvalidConfigValueException exception) {
						throw new ValidationViolationException(exception.getMessage(), exception);
					} catch (UnsupportedException exception) {
						throw new RecordProcessingException(exception.getMessage(), exception);
					}
				}
			}
		}

		return rethinkDBSinkGetResult;
	}

	private static Cursor<? extends Map<String, Object>> getResultSet(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, HashMap<String, Object> row, Object rId,
	        Table table) throws TimeoutException, ImproperValidationException, UnimplementedException, SQLException {
		if (rId == null) {
			return table
			        .filter(new ReqlFilterFunction(row, rethinkDBSinkConfigBean.getKeyFields(), rethinkDBSinkConfigBean.getKeyColumns(),
			                rethinkDBSinkConfigBean.getTimeZoneOffset()))
			        .pluck(rethinkDBSinkConfigBean.getSelectFieldsWithId()).run((Connection) ApplicationConnectionBean.getInstance()
			                .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);
		} else {
			try {
				return table.get(rId).pluck(rethinkDBSinkConfigBean.getSelectFieldsWithId()).run((Connection) ApplicationConnectionBean.getInstance()
				        .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);
			} catch (ReqlNonExistenceError reqlNonExistenceError) {
				return null;
			}
		}
	}

	public static MapObject getRRecord(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, HashMap<String, Object> row, Object rId,
	        NoSQLConstantsConfigBean constants, ArrayList<ExpressionEvaluator> expressionEvaluators)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, DataCorruptedException,
	        ValidationViolationException, InvalidConfigValueException {
		HashMap<String, Object> differKeysAndFieldsRow = RowUtil.changeFieldsToDifferColumns(row, rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getKeyColumns(), rethinkDBSinkConfigBean.getFieldsDifferToColumns(),
		        rethinkDBSinkConfigBean.getColumnsDifferToFields());

		HashMap<String, Object> newRow = RowUtil.addConstantElements(differKeysAndFieldsRow,
		        NoSQLConstantsService.getConstants(rethinkDBSinkConfigBean.getExternalDataPrefix(), rethinkDBSinkConfigBean.getEmptyRow(),
		                rethinkDBSinkConfigBean.getEmptyArguments(), rethinkDBSinkConfigBean.getTimeZoneOffset(), constants, expressionEvaluators));

		if (rId != null) {
			newRow.put("id", rId);
		}

		RethinkDBSinkService.convertToRethinkDBType(newRow, rethinkDBSinkConfigBean.getTimeZoneOffset());

		MapObject rRow = getRRecord(newRow);

		return rRow;
	}

	@SuppressWarnings("unchecked")
	private static MapObject getRRecord(HashMap<String, Object> row) {
		MapObject rRow = RethinkDB.r.hashMap();
		rRow.putAll(row);
		return rRow;
	}
}
