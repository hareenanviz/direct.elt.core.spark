package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentSymmetricPairFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.spark.operation.config.bean.RepartitionConfigBean;
import com.anvizent.elt.core.spark.operation.service.PairFunctionService;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RepartitionPairFunction extends BaseAnvizentSymmetricPairFunction<HashMap<String, Object>, ArrayList<Object>> {

	private static final long serialVersionUID = 1L;

	public RepartitionPairFunction(RepartitionConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
			ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
			throws InvalidArgumentsException {
		super(configBean, null, structure, structure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public Tuple2<ArrayList<Object>, HashMap<String, Object>> process(HashMap<String, Object> row)
			throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		ArrayList<String> keyFields = ((RepartitionConfigBean) configBean).getKeyFields();

		return new Tuple2<ArrayList<Object>, HashMap<String, Object>>(PairFunctionService.getKeyFieldValues(keyFields, row), row);
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}
