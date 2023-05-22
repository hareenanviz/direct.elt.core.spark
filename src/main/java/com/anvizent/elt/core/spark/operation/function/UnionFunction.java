package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

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

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class UnionFunction extends AnvizentFunction {

	private static final long serialVersionUID = 1L;

	public UnionFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidArgumentsException {
		super(configBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		HashMap<String, Object> newRow = new HashMap<>();

		ArrayList<String> rowKeySet = new ArrayList<>(structure.keySet());
		ArrayList<String> newStructureKeySet = new ArrayList<>(newStructure.keySet());

		for (int i = 0; i < rowKeySet.size(); i++) {
			newRow.put(newStructureKeySet.get(i), row.get(rowKeySet.get(i)));
		}

		return newRow;
	}
}
