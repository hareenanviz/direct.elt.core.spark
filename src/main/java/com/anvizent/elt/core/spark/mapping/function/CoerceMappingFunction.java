package com.anvizent.elt.core.spark.mapping.function;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.lang.StringUtils;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.CoerceException;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.MappingFunction;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Coerce;
import com.anvizent.elt.core.spark.mapping.config.bean.CoerceConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CoerceMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private String name;
	@SuppressWarnings("unused")
	private String streamName;

	public CoerceMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		try {
			HashMap<String, Object> newRow = new HashMap<>(row);

			coerce(newRow, row);

			return newRow;
		} catch (InvalidSituationException | UnsupportedCoerceException exception) {
			if (exception.getCause() != null && exception.getCause().getClass().equals(NumberFormatException.class)) {
				throw new DataCorruptedException(exception.getCause().getMessage(), exception.getCause());
			} else {
				throw new DataCorruptedException(exception.getMessage(), exception);
			}
		} catch (InvalidConfigValueException | DateParseException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private void coerce(HashMap<String, Object> newRow, HashMap<String, Object> row) throws DateParseException, UnsupportedCoerceException,
	        InvalidSituationException, ImproperValidationException, InvalidConfigValueException, CoerceException {
		CoerceConfigBean coerceConfigBean = (CoerceConfigBean) mappingConfigBean;

		for (int i = 0; i < coerceConfigBean.getFields().size(); i++) {
			String field = coerceConfigBean.getFields().get(i);
			if (row.containsKey(field)) {
				coerceAndPut(field, i, row, newRow);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void coerceAndPut(String field, int i, HashMap<String, Object> row, HashMap<String, Object> newRow) throws DateParseException,
	        UnsupportedCoerceException, InvalidSituationException, ImproperValidationException, InvalidConfigValueException, CoerceException {
		CoerceConfigBean coerceConfigBean = (CoerceConfigBean) mappingConfigBean;
		Object fieldValue = row.get(field);

		if (fieldValue != null) {
			Class fieldType = fieldValue.getClass();
			newRow.put(field, coerce(field, fieldValue, fieldType, coerceConfigBean.getTypes().get(i),
			        coerceConfigBean.getScales() != null ? coerceConfigBean.getScales().get(i) : null, getCoerceFormat(i)));
		} else {
			newRow.put(field, fieldValue);
		}
	}

	private String getCoerceFormat(int i) {
		CoerceConfigBean coerceConfigBean = (CoerceConfigBean) mappingConfigBean;
		if (coerceConfigBean.getFormats() != null && !coerceConfigBean.getFormats().isEmpty()) {
			return coerceConfigBean.getFormats().get(i);
		} else {
			return null;
		}
	}

	@SuppressWarnings("rawtypes")
	private Object coerce(String field, Object fieldValue, Class fieldType, Class coerceToType, Integer scale, String coerceToFormat) throws DateParseException,
	        UnsupportedCoerceException, InvalidSituationException, ImproperValidationException, InvalidConfigValueException, CoerceException {
		try {
			if (StringUtils.isBlank(coerceToFormat)) {
				return TypeConversionUtil.dataTypeConversion(fieldValue, fieldType, coerceToType, coerceToFormat, scale, Coerce.COERCE_TO_FORMAT,
				        OffsetDateTime.now().getOffset());
			} else {
				try {
					return TypeConversionUtil.dateTypeConversion(fieldValue, fieldType, coerceToType, coerceToFormat, Coerce.COERCE_TO_FORMAT);
				} catch (Exception e) {
					return null;
				}
			}
		} catch (Exception e) {
			if (e.getCause() != null && e.getCause().getCause() instanceof DataCorruptedException) {
				throw new CoerceException("Error while coercing field '" + field + "', value '" + fieldValue + "', message: " + e.getMessage(), e.getCause());
			} else {
				throw new CoerceException("Error while coercing field '" + field + "', value '" + fieldValue + "', message: " + e.getMessage(),
				        new DataCorruptedException(e));
			}
		}
	}

}
