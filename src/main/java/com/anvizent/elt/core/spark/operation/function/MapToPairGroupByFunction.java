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
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentSymmetricPairFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.spark.operation.config.bean.GroupByConfigBean;
import com.anvizent.elt.core.spark.operation.service.PairFunctionService;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MapToPairGroupByFunction extends BaseAnvizentSymmetricPairFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	public MapToPairGroupByFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public Tuple2<HashMap<String, Object>, HashMap<String, Object>> process(HashMap<String, Object> row)
			throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		GroupByConfigBean groupByConfigBean = (GroupByConfigBean) configBean;

		return new Tuple2<HashMap<String, Object>, HashMap<String, Object>>(
				PairFunctionService.getKeyFieldsAndValues(groupByConfigBean.getGroupByFields(), row), new HashMap<String, Object>(row));
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}
