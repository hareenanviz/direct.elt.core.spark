package com.anvizent.elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.MappingFunction;
import com.anvizent.elt.core.spark.constant.Granularity;
import com.anvizent.elt.core.spark.mapping.config.bean.DateAndTimeGranularityConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DateAndTimeGranularityMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;

	public DateAndTimeGranularityMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		HashMap<String, Object> newRow = new HashMap<>(row);
		DateAndTimeGranularityConfigBean configBean = (DateAndTimeGranularityConfigBean) mappingConfigBean;

		if (configBean.isAllDateFields()) {
			everyDateFieldGranularityCleansing(newRow, row);
		} else {
			providedFieldsGranularityCleansing(newRow, row);
		}

		return newRow;
	}

	private void providedFieldsGranularityCleansing(HashMap<String, Object> newRow, HashMap<String, Object> row) {
		DateAndTimeGranularityConfigBean configBean = (DateAndTimeGranularityConfigBean) mappingConfigBean;

		for (int i = 0; i < configBean.getFields().size(); i++) {
			String key = configBean.getFields().get(i);
			if (row.containsKey(key)) {
				Object value = row.get(key);
				if (value != null && value.getClass().equals(Date.class)) {
					newRow.put(key, granularityCleansing(row, value, i));
				}
			}
		}
	}

	private void everyDateFieldGranularityCleansing(HashMap<String, Object> newRow, HashMap<String, Object> row) {
		for (Entry<String, Object> entry : row.entrySet()) {
			if (entry.getValue() != null && entry.getValue().getClass().equals(Date.class)) {
				newRow.put(entry.getKey(), granularityCleansing(row, entry.getValue(), 0));
			}
		}
	}

	private Object granularityCleansing(HashMap<String, Object> row, Object value, int index) {
		DateAndTimeGranularityConfigBean configBean = (DateAndTimeGranularityConfigBean) mappingConfigBean;

		Calendar calendar = Calendar.getInstance();
		calendar.setTime((Date) value);

		if (configBean.getGranularities().get(index).equals(Granularity.SECOND)) {
			calendar.set(Calendar.MILLISECOND, 0);
		} else if (configBean.getGranularities().get(index).equals(Granularity.MINUTE)) {
			calendar.set(Calendar.MILLISECOND, 0);
			calendar.set(Calendar.SECOND, 0);
		} else if (configBean.getGranularities().get(index).equals(Granularity.HOUR)) {
			calendar.set(Calendar.MILLISECOND, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MINUTE, 0);
		} else if (configBean.getGranularities().get(index).equals(Granularity.DAY)) {
			calendar.set(Calendar.MILLISECOND, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
		} else if (configBean.getGranularities().get(index).equals(Granularity.MONTH)) {
			calendar.set(Calendar.MILLISECOND, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.DAY_OF_MONTH, 1);
		} else {
			calendar.set(Calendar.MILLISECOND, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			calendar.set(Calendar.MONTH, 0);
		}

		return new Date(calendar.getTimeInMillis());
	}
}
