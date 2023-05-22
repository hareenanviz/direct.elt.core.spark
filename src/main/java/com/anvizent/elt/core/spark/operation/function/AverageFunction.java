package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.exception.AggregationException;
import com.anvizent.elt.core.spark.operation.service.AggregationService;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class AverageFunction extends AnvizentFunction {

	private static final long serialVersionUID = 1L;

	private HashMap<String, String[]> averageFields;

	public AverageFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
			HashMap<String, String[]> averageFields, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction anvizentVoidFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, null, structure, newStructure, null, accumulators, anvizentVoidFunction, jobDetails);
		this.averageFields = averageFields;
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		HashMap<String, Object> newRow = new HashMap<String, Object>(row);

		if (averageFields == null || averageFields.isEmpty()) {
			return newRow;
		}

		return putAverageFields(row, newRow);
	}

	private HashMap<String, Object> putAverageFields(HashMap<String, Object> row, HashMap<String, Object> newRow) throws DataCorruptedException {
		try {
			for (Entry<String, String[]> entry : averageFields.entrySet()) {
				String aggregateFieldAlias = entry.getKey();
				String averageSumField = entry.getValue()[0];
				String averageCountField = entry.getValue()[1];

				newRow.remove(averageSumField);
				newRow.remove(averageCountField);

				newRow.put(aggregateFieldAlias, AggregationService.average(row.get(averageSumField), row.get(averageCountField)));
			}
			return newRow;
		} catch (AggregationException aggregationException) {
			throw new DataCorruptedException(aggregationException);
		}
	}
}
