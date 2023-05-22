package com.anvizent.elt.core.spark.mapping.function;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.MappingFunction;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class EveryDateFormatterMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;
	private String dateFormat = null;

	public EveryDateFormatterMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, String format,
			LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure, AnvizentVoidFunction anvizentVoidFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, null, anvizentVoidFunction, jobDetails);
		dateFormat = format;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException {
		for (Entry<String, AnvizentDataType> map : newStructure.entrySet()) {
			String key = map.getKey();
			Class valueType = map.getValue().getJavaType();
			if (valueType.equals(Date.class)) {
				row.put(key, TypeConversionUtil.dateToStringConversion((Date) row.get(key), String.class, dateFormat));
			}
		}

		return row;
	}
}
