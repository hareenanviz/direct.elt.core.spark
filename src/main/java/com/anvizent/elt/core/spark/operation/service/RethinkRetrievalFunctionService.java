package com.anvizent.elt.core.spark.operation.service;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

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
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.RethinkLookUp;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;
import com.anvizent.elt.core.spark.sink.service.ReqlFilterFunction;
import com.anvizent.elt.core.spark.sink.service.RethinkDBSinkService;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Pluck;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class RethinkRetrievalFunctionService {

	public static HashMap<String, Object> getLookUpRow(HashMap<String, Object> row, Table table, RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
	        throws InvalidLookUpException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        InvalidConfigValueException, UnimplementedException, SQLException, TimeoutException {
		HashMap<String, Object> newObject = new HashMap<>();
		HashMap<String, Object> newRow = new HashMap<>();

		Pluck pluck = table
		        .filter(new ReqlFilterFunction(row, rethinkRetrievalConfigBean.getEqFields(), rethinkRetrievalConfigBean.getEqColumns(),
		                rethinkRetrievalConfigBean.getLtFields(), rethinkRetrievalConfigBean.getLtColumns(), rethinkRetrievalConfigBean.getGtFields(),
		                rethinkRetrievalConfigBean.getGtColumns(), rethinkRetrievalConfigBean.getTimeZoneOffset()))
		        .pluck(rethinkRetrievalConfigBean.getSelectFields());

		Cursor<? extends Map<String, Object>> data = pluck.run((Connection) ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkRetrievalConfigBean.getRethinkDBConnection(), null, TaskContext.getPartitionId()), true)[0]);

		int rowsCount = 0;

		if (data.hasNext()) {
			++rowsCount;
			newRow.putAll(convertToSelectFieldTypes(data.next(), rethinkRetrievalConfigBean.getSelectFields(), rethinkRetrievalConfigBean.getSelectFieldTypes(),
			        rethinkRetrievalConfigBean.getSelectFieldDateFormats(), rethinkRetrievalConfigBean.getTimeZoneOffset()));

			if (data.hasNext() && ((RethinkLookUpConfigBean) rethinkRetrievalConfigBean).isLimitTo1()) {
				throw new InvalidLookUpException(
				        MessageFormat.format(ExceptionMessage.MORE_THAN_ONE_ROW_IN_LOOKUP_TABLE, rethinkRetrievalConfigBean.getTableName()));
			}
		}

		newObject.put(General.LOOKUP_CACHED, false);
		newObject.put(General.LOOKUP_RESULTING_ROW, newRow);
		newObject.put(General.LOOKEDUP_ROWS_COUNT, rowsCount);

		return newObject;
	}

	private static HashMap<String, Object> convertToSelectFieldTypes(Map<String, Object> selectRow, ArrayList<String> selectFields,
	        ArrayList<AnvizentDataType> selectFieldTypes, ArrayList<String> selectFieldDateFormats, ZoneOffset timeZoneOffset)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		HashMap<String, Object> newRow = new HashMap<>();

		for (int i = 0; i < selectFields.size(); i++) {
			Object fieldValue = selectRow.get(selectFields.get(i));
			newRow.put(selectFields.get(i), convertToSelectFieldType(fieldValue, selectFieldTypes.get(i).getJavaType(),
			        getSelectDateFormat(i, selectFieldDateFormats), timeZoneOffset));
		}

		return newRow;
	}

	public static String getSelectDateFormat(int i, ArrayList<String> selectFieldDateFormats) {
		if (selectFieldDateFormats != null && !selectFieldDateFormats.isEmpty()) {
			return selectFieldDateFormats.get(i);
		} else {
			return null;
		}
	}

	public static Object convertToSelectFieldType(Object fieldValue, Class fieldToType, String selectFieldDateFormat, ZoneOffset timeZoneOffset)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		if (fieldValue == null) {
			return null;
		} else {
			return TypeConversionUtil.dataTypeConversion(fieldValue, fieldValue.getClass(), fieldToType, selectFieldDateFormat, null,
			        RethinkLookUp.SELECT_FIELD_DATE_FORMATS, timeZoneOffset);
		}
	}

	public static HashMap<String, Object> checkInsertOnZeroFetch(HashMap<String, Object> row, Table table,
	        RethinkRetrievalConfigBean rethinkRetrievalConfigBean) throws InvalidLookUpException, UnsupportedCoerceException, InvalidSituationException,
	        DateParseException, ImproperValidationException, InvalidConfigValueException, UnimplementedException, SQLException, TimeoutException {
		HashMap<String, Object> newRow = new HashMap<>();

		if (rethinkRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			insertOnZeroFetch(row, table, rethinkRetrievalConfigBean, newRow);
		} else if (rethinkRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.FAIL)) {
			throw new InvalidLookUpException(ExceptionMessage.FAILED_ON_ZERO_FETCH);
		} else {
			return newRow;
		}

		return newRow;
	}

	private static void insertOnZeroFetch(HashMap<String, Object> row, Table table, RethinkRetrievalConfigBean rethinkRetrievalConfigBean,
	        HashMap<String, Object> newRow) throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
	        InvalidConfigValueException, UnimplementedException, SQLException, TimeoutException {
		HashMap<String, Object> whereKeyValues = CollectionUtil.getSubMap(row, rethinkRetrievalConfigBean.getWhereFields(),
		        (rethinkRetrievalConfigBean.getWhereColumns() == null || rethinkRetrievalConfigBean.getWhereColumns().isEmpty())
		                ? rethinkRetrievalConfigBean.getWhereFields()
		                : rethinkRetrievalConfigBean.getWhereColumns());

		insert(row, table, rethinkRetrievalConfigBean, whereKeyValues);

		putNewRow(newRow, rethinkRetrievalConfigBean, whereKeyValues);
	}

	private static void putNewRow(HashMap<String, Object> newRow, RethinkRetrievalConfigBean rethinkRetrievalConfigBean,
	        HashMap<String, Object> whereKeyValues) {
		newRow.putAll(CollectionUtil.generateMap(rethinkRetrievalConfigBean.getSelectFields(), rethinkRetrievalConfigBean.getOutputInsertValues()));
		newRow.putAll(whereKeyValues);
	}

	private static void insert(HashMap<String, Object> row, Table table, RethinkRetrievalConfigBean rethinkRetrievalConfigBean,
	        HashMap<String, Object> whereKeyValues) throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException,
	        UnsupportedCoerceException, InvalidSituationException, DateParseException, InvalidConfigValueException {
		HashMap<String, Object> insertValues = CollectionUtil.generateMap(rethinkRetrievalConfigBean.getSelectFields(),
		        rethinkRetrievalConfigBean.getTableInsertValues());

		HashMap<String, Object> insertRow = new LinkedHashMap<>(insertValues);
		RethinkDBSinkService.convertToRethinkDBType(whereKeyValues, rethinkRetrievalConfigBean.getTimeZoneOffset());
		insertRow.putAll(whereKeyValues);

		table.insert(getRRecord(insertRow)).run((Connection) ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkRetrievalConfigBean.getRethinkDBConnection(), null, TaskContext.getPartitionId()), true)[0]);
	}

	public static HashMap<String, Object> replaceWithAliasNames(HashMap<String, Object> newRow, ArrayList<String> selectFields,
	        ArrayList<String> selectFieldAliases) {
		if (selectFieldAliases == null || selectFieldAliases.isEmpty()) {
			return newRow;
		}

		HashMap<String, Object> aliasedNewRow = new HashMap<>();

		for (int i = 0; i < selectFieldAliases.size(); i++) {
			aliasedNewRow.put(selectFieldAliases.get(i), newRow.get(selectFields.get(i)));
		}

		return aliasedNewRow;
	}

	private static MapObject getRRecord(HashMap<String, Object> record) {
		MapObject rRecord = RethinkDB.r.hashMap();
		rRecord.putAll(record);
		return rRecord;
	}

	public static HashMap<String, Object> getFetcherRows(HashMap<String, Object> row, Table table, RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException,
	        InvalidLookUpException, UnimplementedException, SQLException, TimeoutException {
		HashMap<String, Object> newObject = new HashMap<>();
		ArrayList<HashMap<String, Object>> newRows = new ArrayList<>();

		Pluck pluck = table
		        .filter(new ReqlFilterFunction(row, rethinkRetrievalConfigBean.getEqFields(), rethinkRetrievalConfigBean.getEqColumns(),
		                rethinkRetrievalConfigBean.getLtFields(), rethinkRetrievalConfigBean.getLtColumns(), rethinkRetrievalConfigBean.getGtFields(),
		                rethinkRetrievalConfigBean.getGtColumns(), rethinkRetrievalConfigBean.getTimeZoneOffset()))
		        .pluck(rethinkRetrievalConfigBean.getSelectFields());

		Cursor<? extends Map<String, Object>> data = pluck.run((Connection) ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkRetrievalConfigBean.getRethinkDBConnection(), null, TaskContext.getPartitionId()), true)[0]);

		int rowsCount = 0;

		while (data.hasNext() && ++rowsCount <= ((RethinkFetcherConfigBean) rethinkRetrievalConfigBean).getMaxFetchLimit()) {
			newRows.add(convertToSelectFieldTypes(data.next(), rethinkRetrievalConfigBean.getSelectFields(), rethinkRetrievalConfigBean.getSelectFieldTypes(),
			        rethinkRetrievalConfigBean.getSelectFieldDateFormats(), rethinkRetrievalConfigBean.getTimeZoneOffset()));
		}

		if (data.hasNext()) {
			throw new InvalidLookUpException(MessageFormat.format(ExceptionMessage.MORE_THAN_EXPECTED_ROWS_IN_FETCHER_TABLE,
			        ((RethinkFetcherConfigBean) rethinkRetrievalConfigBean).getMaxFetchLimit(), rethinkRetrievalConfigBean.getTableName()));
		}

		newObject.put(General.LOOKUP_CACHED, false);
		newObject.put(General.LOOKUP_RESULTING_ROW, newRows);
		newObject.put(General.LOOKEDUP_ROWS_COUNT, rowsCount);

		return newObject;
	}
}
