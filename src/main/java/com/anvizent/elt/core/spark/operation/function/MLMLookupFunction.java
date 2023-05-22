package com.anvizent.elt.core.spark.operation.function;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseDBInsertableAnvizentFlatMapFunction;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.lib.stats.beans.BaseDBRetrievalRow;
import com.anvizent.elt.core.lib.stats.beans.DBRetrievalRow;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.MLM;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.operation.config.bean.MLMLookupConfigBean;
import com.anvizent.elt.core.spark.operation.exception.MLMResponseException;
import com.anvizent.elt.core.spark.operation.service.MLMLookupService;
import com.anvizent.elt.core.spark.row.formatter.AnvizentZipperErrorSetter;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.anvizent.rest.util.RestUtil;
import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;

import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;
import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class MLMLookupFunction extends BaseDBInsertableAnvizentFlatMapFunction<Tuple2<HashMap<String, Object>, Long>, HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;
	private static final String TYPE = "type";
	private static final String SELECTOR = "selector";
	private static final String DIMENSION = "dimension";
	private static final String VALUE = "value";
	private static final String AND = "and";
	private static final String FIELDS = "fields";
	private static final String SAMPLE_QUERY = "{ \"queryType\": \"select\", \"dataSource\": \"druid-kafka-test-incr-emulator\", \"granularity\": \"all\", "
	        + "\"dimensions\": [], \"metrics\": [], \"pagingSpec\": { \"threshold\": null, \"pagingIdentifiers\": {} }, "
	        + "\"intervals\": [ \"1967-01-01T00:00:00.000/2019-01-01T00:00:00.000\" ] }";
	private static final String DATA_SOURCE = "dataSource";
	private static final String FILTER = "filter";
	private static final String DATA = "data";
	private static final String RESULT = "result";
	private static final String EVENTS = "events";
	private static final String EVENT = "event";
	private final EthernetAddress ethernetAddress;
	private final SimpleDateFormat dateFormat;

	public MLMLookupFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException, NumberFormatException, SocketException, UnknownHostException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);

		ethernetAddress = new EthernetAddress(NetworkInterface.getByInetAddress(InetAddress.getLocalHost()).getHardwareAddress());
		MLMLookupConfigBean mlmLookupConfigBean = (MLMLookupConfigBean) configBean;
		if (mlmLookupConfigBean.isIncremental()) {
			dateFormat = new SimpleDateFormat(mlmLookupConfigBean.getDruidTimeFormat());
		} else {
			dateFormat = null;
		}
	}

	@Override
	public BaseDBRetrievalRow<Tuple2<HashMap<String, Object>, Long>, Iterator<HashMap<String, Object>>> process(Tuple2<HashMap<String, Object>, Long> tuple2)
	        throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		MLMLookupConfigBean mlmLookupConfigBean = (MLMLookupConfigBean) configBean;

		try {
			HashMap<String, Object> newRow = getNewRow(tuple2);
			ArrayList<HashMap<String, Object>> newRows = new ArrayList<>();
			HashMap<String, Object> nutralizerRow = null;

			if (mlmLookupConfigBean.isIncremental()) {
				nutralizerRow = getNutralizerRow(mlmLookupConfigBean, tuple2._1());
				if (nutralizerRow != null) {
					newRows.add(nutralizerRow);
				}
			}

			newRows.add(newRow);

			return new DBRetrievalRow<Tuple2<HashMap<String, Object>, Long>, Iterator<HashMap<String, Object>>>(tuple2, newRows.iterator(), false, false,
			        newRows.size(), nutralizerRow == null ? 0 : 1);
		} catch (Exception exception) {
			if (exception instanceof RecordProcessingException) {
				throw (RecordProcessingException) exception;
			} else if (exception instanceof ValidationViolationException) {
				throw (ValidationViolationException) exception;
			} else if (exception instanceof DataCorruptedException) {
				throw (DataCorruptedException) exception;
			} else {
				throw new DataCorruptedException(exception.getMessage(), exception);
			}
		}
	}

	private HashMap<String, Object> getNutralizerRow(MLMLookupConfigBean mlmLookupConfigBean, HashMap<String, Object> row) throws Exception {
		HashMap<String, Object> originalRow = getOriginalRow(mlmLookupConfigBean, row);
		HashMap<String, Object> nutralizerRow = getNutralizerRow(originalRow);

		if (nutralizerRow != null) {
			return RowUtil.addElements(nutralizerRow, newStructure);
		} else {
			return null;
		}
	}

	@SuppressWarnings("deprecation")
	private HashMap<String, Object> getNutralizerRow(HashMap<String, Object> originalRow) throws RecordProcessingException, ParseException {
		if (originalRow == null || originalRow.size() == 0) {
			return null;
		}

		HashMap<String, Object> nutralizerRow = new HashMap<>();

		for (Entry<String, Object> originalField : originalRow.entrySet()) {
			String key = originalField.getKey();
			Object value = originalField.getValue();

			if (key.equals(MLM.ANVIZ_UUID)) {
				nutralizerRow.put(MLM.ANVIZ_COPY_OF, originalRow.get(MLM.ANVIZ_UUID));
				try {
					nutralizerRow.put(MLM.ANVIZ_UUID, Generators.timeBasedGenerator(ethernetAddress).generate().toString());
				} catch (Exception exception) {
					throw new RecordProcessingException("Unable to generate UUID", exception);
				}
			} else if (key.equals(MLM.DRUID_TIME_FIELD)) {
				nutralizerRow.put(MLM.TIME_FIELD, dateFormat.parse((String) value).getTime() - new java.util.Date(0).getTimezoneOffset() * 60 * 1000);
			} else if (!Constants.Type.DIMENSIONS.contains(value.getClass())) {
				nutralizerRow.put(key, TypeConversionUtil.getNegativeValue(value, key, configBean));
			} else {
				nutralizerRow.put(key, value);
			}
		}

		return nutralizerRow;
	}

	private HashMap<String, Object> getOriginalRow(MLMLookupConfigBean mlmLookupConfigBean, HashMap<String, Object> row) throws Exception {
		HashMap<String, Object> query = getLookupQuery(mlmLookupConfigBean, row);
		HashMap<String, Object> result = sendRequest(mlmLookupConfigBean, query);

		return getOriginalRow(result);
	}

	private LinkedHashMap<String, Object> getOriginalRow(HashMap<String, Object> result) throws MLMResponseException {
		if (result.get(DATA) == null) {
			return null;
		} else {
			if (!(result.get(DATA) instanceof List)) {
				throw new MLMResponseException(
				        "Invalid response found from MLM, expected data to be " + List.class + ", found: " + result.get(DATA).getClass());
			} else {
				return getOriginalRowFromData((List<Map<String, Object>>) result.get(DATA));
			}
		}
	}

	private LinkedHashMap<String, Object> getOriginalRowFromData(List<Map<String, Object>> data) throws MLMResponseException {
		if (data.size() == 0) {
			return null;
		} else if (data.size() > 1) {
			throw new MLMResponseException("More than 1 record found in lookup");
		} else if (data.get(0).get(RESULT) == null) {
			throw new MLMResponseException("Invalid response from druid data.result");
		} else if (!(data.get(0).get(RESULT) instanceof Map)) {
			throw new MLMResponseException(
			        "Invalid response found from MLM, expected data.result to be " + Map.class + ", found: " + data.get(0).get(RESULT).getClass());
		} else {
			Map<String, Object> dataResult = (Map<String, Object>) data.get(0).get(RESULT);
			return getOriginalRowFromDataResult(dataResult);
		}
	}

	private LinkedHashMap<String, Object> getOriginalRowFromDataResult(Map<String, Object> dataResult) throws MLMResponseException {
		if (dataResult.get(EVENTS) == null) {
			return null;
		} else {
			if (!(dataResult.get(EVENTS) instanceof List)) {
				throw new MLMResponseException(
				        "Invalid response found from MLM, expected data.result.events to be " + List.class + ", found: " + dataResult.get(EVENTS).getClass());
			} else {
				return getOriginalRowFromEvents((List<Map<String, Object>>) dataResult.get(EVENTS));
			}
		}
	}

	private LinkedHashMap<String, Object> getOriginalRowFromEvents(List<Map<String, Object>> events) throws MLMResponseException {
		if (events.size() == 0) {
			return null;
		} else if (events.size() > 1) {
			throw new MLMResponseException("More than 1 record found in lookup");
		} else if (events.get(0).get(EVENT) == null) {
			throw new MLMResponseException("Invalid response from druid data.result.events.event not found");
		} else if (!(events.get(0).get(EVENT) instanceof Map)) {
			throw new MLMResponseException("Invalid response found from druid, expected data.result.events.event to be " + Map.class + ", found: "
			        + events.get(0).get(EVENT).getClass());
		} else {
			return new LinkedHashMap<>((Map<String, Object>) events.get(0).get(EVENT));
		}
	}

	private HashMap<String, Object> sendRequest(MLMLookupConfigBean mlmLookupConfigBean, HashMap<String, Object> query) throws Exception {
		RestUtil restUtil = new RestUtil(true);

		ResponseEntity<String> exchange = restUtil.exchange(mlmLookupConfigBean.getQueryURL(),
		        new JSONSerializer().exclude("*.class").include("*.*").serialize(query), HttpMethod.POST, MLMLookupService.getHeadersMap(mlmLookupConfigBean),
		        mlmLookupConfigBean.getUserId(), mlmLookupConfigBean.getClientId());

		MLMLookupService.validateResponse(exchange, mlmLookupConfigBean.getQueryURL(), "query using");

		return new JSONDeserializer<HashMap<String, Object>>().deserialize(exchange.getBody());
	}

	private HashMap<String, Object> getLookupQuery(MLMLookupConfigBean mlmLookupConfigBean, HashMap<String, Object> row) throws DataCorruptedException {
		HashMap<String, Object> filter = getFilter(mlmLookupConfigBean, row);

		HashMap<String, Object> query = new JSONDeserializer<HashMap<String, Object>>().deserialize(SAMPLE_QUERY);
		query.put(DATA_SOURCE, mlmLookupConfigBean.getTableName());
		query.put(FILTER, filter);

		return query;
	}

	private HashMap<String, Object> getFilter(MLMLookupConfigBean mlmLookupConfigBean, HashMap<String, Object> row) throws DataCorruptedException {
		HashMap<String, Object> filter = new HashMap<>();

		filter.put(TYPE, AND);
		filter.put(FIELDS, getFilters(mlmLookupConfigBean, row));

		return filter;
	}

	private ArrayList<HashMap<String, Object>> getFilters(MLMLookupConfigBean mlmLookupConfigBean, HashMap<String, Object> row) throws DataCorruptedException {
		ArrayList<HashMap<String, Object>> filters = new ArrayList<>();
		for (String keyField : mlmLookupConfigBean.getKeyFields()) {
			Object keyValue = row.get(keyField);
			if (keyValue != null) {
				filters.add(getEqFilter(keyField, keyValue));
			}
		}

		if (filters.isEmpty()) {
			throw new DataCorruptedException("No key field is present in incoming row");
		}

		return filters;
	}

	private HashMap<String, Object> getEqFilter(String keyField, Object keyValue) {
		HashMap<String, Object> filter = new HashMap<>();

		filter.put(TYPE, SELECTOR);
		filter.put(DIMENSION, keyField);
		filter.put(VALUE, keyValue);

		return filter;
	}

	private HashMap<String, Object> getNewRow(Tuple2<HashMap<String, Object>, Long> tuple2) throws RecordProcessingException, InvalidConfigValueException {
		HashMap<String, Object> mlmRow = new HashMap<String, Object>();
		duplicateMeasures(tuple2._1(), mlmRow);
		addAnvizFields(tuple2._2(), mlmRow);
		return RowUtil.addElements(tuple2._1(), mlmRow, newStructure);
	}

	private void addAnvizFields(Long id, HashMap<String, Object> newValues) throws RecordProcessingException {
		newValues.put(MLM.ANVIZ_COUNT, 1);
		try {
			newValues.put(MLM.ANVIZ_UUID, Generators.timeBasedGenerator(ethernetAddress).generate().toString());
		} catch (Exception exception) {
			throw new RecordProcessingException("Unable to generate UUID", exception);
		}
		newValues.put(MLM.TIME_FIELD, id * MLM.TIME_FIELD_STEP);
	}

	private void duplicateMeasures(HashMap<String, Object> row, HashMap<String, Object> newValues)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		for (Entry<String, Object> rowEntry : row.entrySet()) {
			Object fieldValue = rowEntry.getValue();
			if (fieldValue != null && !Constants.Type.DIMENSIONS.contains(fieldValue.getClass())) {
				newValues.put(rowEntry.getKey() + MLM.MEASURES_AS_DIMENSIONS_POST_FIX_DEFAULT,
				        TypeConversionUtil.dataTypeConversion(fieldValue, fieldValue.getClass(), String.class, null, null, null, null));

			}
		}
	}

	@Override
	protected BaseAnvizentErrorSetter<Tuple2<HashMap<String, Object>, Long>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter()
	        throws InvalidArgumentsException {
		return new AnvizentZipperErrorSetter();
	}
}