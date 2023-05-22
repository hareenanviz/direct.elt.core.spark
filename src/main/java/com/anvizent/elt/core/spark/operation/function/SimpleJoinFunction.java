package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;
import com.anvizent.elt.core.spark.row.formatter.SimpleJoinFunctionErrorSetter;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SimpleJoinFunction extends JoinFunction<Tuple2<HashMap<String, Object>, HashMap<String, Object>>> {

	private static final long serialVersionUID = 1L;

	public SimpleJoinFunction(JoinConfigBean joinConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, HashMap<String, String> lhsNewNames, HashMap<String, String> rhsNewNames,
			ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
			throws InvalidArgumentsException {
		super(joinConfigBean, structure, newStructure, lhsNewNames, rhsNewNames, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public HashMap[] getRows(Tuple2<HashMap<String, Object>, HashMap<String, Object>> row) {
		return new HashMap[] { row._1(), row._2() };
	}

	@Override
	protected BaseAnvizentErrorSetter<Tuple2<HashMap<String, Object>, HashMap<String, Object>>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter()
			throws InvalidArgumentsException {
		return new SimpleJoinFunctionErrorSetter();
	}
}
