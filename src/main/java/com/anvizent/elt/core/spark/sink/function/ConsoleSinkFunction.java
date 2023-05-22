package com.anvizent.elt.core.spark.sink.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.AlwaysWrittenRow;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConsoleSinkFunction extends AnvizentVoidFunction {

	private static final long serialVersionUID = 1L;

	public ConsoleSinkFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> anvizentAccumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidRelationException {
		super(configBean, null, structure, newStructure, null, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		LinkedHashMap<String, Object> newRow = new LinkedHashMap<String, Object>(row.size());

		for (Entry<String, AnvizentDataType> structure : newStructure.entrySet()) {
			newRow.put(structure.getKey(), row.get(structure.getKey()));
		}

		System.out.println(newRow);

		return new AlwaysWrittenRow();
	}
}
