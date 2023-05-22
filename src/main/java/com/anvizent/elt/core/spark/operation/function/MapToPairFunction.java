package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentSymmetricPairFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;
import com.anvizent.elt.core.spark.operation.service.PairFunctionService;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MapToPairFunction extends BaseAnvizentSymmetricPairFunction<HashMap<String, Object>, ArrayList<Object>> {
	private static final long serialVersionUID = 1L;

	private ArrayList<String> joinFields;

	public MapToPairFunction(JoinConfigBean joinConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, String LHSOrRHS, ArrayList<AnvizentAccumulator> accumulators,
			AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidArgumentsException {
		super(joinConfigBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);

		if (LHSOrRHS.equals(ConfigConstants.Operation.Join.LEFT_HAND_SIDE)) {
			this.joinFields = joinConfigBean.getLHSFields();
		} else {
			this.joinFields = joinConfigBean.getRHSFields();
		}
	}

	public Tuple2<ArrayList<Object>, HashMap<String, Object>> process(HashMap<String, Object> row) {
		return new Tuple2<ArrayList<Object>, HashMap<String, Object>>(PairFunctionService.getKeyFieldValues(joinFields, row), new LinkedHashMap<>(row));
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}
