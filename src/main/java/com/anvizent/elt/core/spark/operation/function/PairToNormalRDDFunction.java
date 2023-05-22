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
import com.anvizent.elt.core.lib.function.AnvizentPairToNormalRDDFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentSymetricPairToNormalErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class PairToNormalRDDFunction extends AnvizentPairToNormalRDDFunction<HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	public PairToNormalRDDFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		return row;
	}

	@Override
	protected BaseAnvizentErrorSetter<Tuple2<HashMap<String, Object>, HashMap<String, Object>>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter()
			throws InvalidArgumentsException {
		return new AnvizentSymetricPairToNormalErrorSetter<HashMap<String, Object>>();
	}
}
